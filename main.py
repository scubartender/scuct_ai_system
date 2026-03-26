from datetime import datetime

import base64
import hashlib
import hmac
import json
import logging
import os
import re
import threading
import traceback
from contextlib import asynccontextmanager
from datetime import timedelta, timezone

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent, ImageMessageContent, FollowEvent

from config import config
from services.sheets_service import SheetsService
from services.llm_service import (
    extract_invoice_data,
    apply_user_edit,
    parse_manual_record_text,
    set_token_logger,
)
from services.line_service import LineService
from core.state_manager import StateManager, AppState
from core.schemas import InvoiceData

handler = WebhookHandler(config.LINE_CHANNEL_SECRET)
logger = logging.getLogger(__name__)

CONFIRM_TOKENS = {"確認", "确认", "confirm", "ok", "yes"}
EDIT_TOKENS = {"修改", "更正", "edit", "fix"}
CANCEL_TOKENS = {"取消", "cancel", "reset"}
SKIP_TOKENS = {"略過", "跳過", "skip"}
BOOKKEEP_TOKENS = {"記帳", "記賬", "手動記帳", "manual"}
ACTIVATION_TOKEN = "好"
USAGE_GUIDE_TOKENS = {"如何使用"}
AMOUNT_ALERT_THRESHOLD = 50000
AMOUNT_CONFIRM_ACCEPT_TOKENS = {"確認改金額", "改成這個金額", "是"}
AMOUNT_CONFIRM_REJECT_TOKENS = {"不要改金額", "先不要改", "否"}
FINANCE_GUIDE_KEYWORDS = {
    "財務使用教學",
    "財務教學",
    "財務使用說明",
    "財務操作手冊",
    "財務手冊",
}

# Global service instances
_sheets_service = None
_line_service = None
_state_manager = None
_daily_scheduler_thread = None
_hourly_scheduler_thread = None
_scheduler_stop_event = threading.Event()
_debug_log_lock = threading.Lock()
_tw_timezone = timezone(timedelta(hours=8))
_debug_log_path = "debug_error.log"


def _build_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        _start_background_threads()
        try:
            yield
        finally:
            _stop_background_threads()

    return FastAPI(title="Bartending Club Finance Agent", lifespan=lifespan)


app = _build_app()


def _mask_sensitive(value: str, keep_last: int = 4) -> str:
    text = str(value or "").strip()
    if not text:
        return "-"
    if len(text) <= keep_last:
        return "*" * len(text)
    return f"{'*' * (len(text) - keep_last)}{text[-keep_last:]}"


def _rotate_debug_log_if_needed():
    max_bytes = max(0, int(config.DEBUG_ERROR_LOG_MAX_BYTES or 0))
    if max_bytes <= 0:
        return
    if not os.path.exists(_debug_log_path):
        return
    if os.path.getsize(_debug_log_path) <= max_bytes:
        return
    stamp = datetime.now(_tw_timezone).strftime("%Y%m%d_%H%M%S")
    os.replace(_debug_log_path, f"{_debug_log_path}.{stamp}")


def _append_exception_to_debug_log(context: str, exc: Exception, user_id: str = "", reply_token: str = ""):
    exc_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    stamp = datetime.now(_tw_timezone).isoformat()
    with _debug_log_lock:
        _rotate_debug_log_if_needed()
        with open(_debug_log_path, "a", encoding="utf-8") as f:
            f.write(
                f"[{stamp}] context={context} user_id={_mask_sensitive(user_id)} "
                f"reply_token={_mask_sensitive(reply_token)}\n{exc_text}\n"
            )


def _log_runtime_exception(context: str, exc: Exception, event=None):
    user_id = getattr(getattr(event, "source", None), "user_id", "") if event else ""
    reply_token = getattr(event, "reply_token", "") if event else ""
    event_type = type(event).__name__ if event else "None"

    logger.exception(
        "Unhandled exception in %s. user_id=%s reply_token=%s event_type=%s",
        context,
        user_id or "-",
        reply_token or "-",
        event_type,
    )
    try:
        _append_exception_to_debug_log(context, exc, user_id=user_id, reply_token=reply_token)
    except Exception:
        logger.exception("Failed writing debug_error.log for context=%s", context)


def _notify_user_runtime_error(event, message: str = "系統目前忙碌，請稍後再試一次。"):
    user_id = getattr(getattr(event, "source", None), "user_id", None)
    reply_token = getattr(event, "reply_token", None)

    try:
        line_service = get_line_service()
    except Exception as service_exc:
        _log_runtime_exception("notify_user_runtime_error_get_services", service_exc, event=event)
        return

    if reply_token:
        try:
            line_service.reply_text(reply_token, message)
            return
        except Exception as reply_exc:
            _log_runtime_exception("notify_user_runtime_error_reply", reply_exc, event=event)

    if user_id:
        try:
            line_service.push_text(user_id, message)
        except Exception as push_exc:
            _log_runtime_exception("notify_user_runtime_error_push", push_exc, event=event)


def _handle_event_exception(context: str, event, exc: Exception, notify_user: bool = True):
    _log_runtime_exception(context, exc, event=event)
    if notify_user:
        _notify_user_runtime_error(event)


def _safe_handle_webhook(body_decoded: str, signature: str):
    try:
        handler.handle(body_decoded, signature)
    except InvalidSignatureError as e:
        _log_runtime_exception("webhook_background_invalid_signature", e, event=None)
    except Exception as e:
        _log_runtime_exception("webhook_background_unhandled", e, event=None)


def _is_valid_line_signature(body_decoded: str, signature: str) -> bool:
    secret = (config.LINE_CHANNEL_SECRET or "").encode("utf-8")
    if not secret:
        return False
    digest = hmac.new(secret, body_decoded.encode("utf-8"), hashlib.sha256).digest()
    expected_signature = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(expected_signature, signature or "")


def _seconds_until_next_reset(hour: int, minute: int) -> float:
    now = datetime.now(_tw_timezone)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


def _daily_state_reset_loop():
    hour = int(config.DAILY_STATE_RESET_HOUR)
    minute = int(config.DAILY_STATE_RESET_MINUTE)
    while not _scheduler_stop_event.is_set():
        wait_seconds = _seconds_until_next_reset(hour, minute)
        if _scheduler_stop_event.wait(wait_seconds):
            return
        try:
            sheets_service, _, _ = get_services()
            result = sheets_service.reset_all_states_to_normal(reason="daily_8am_cleanup")
            logger.info(
                "Daily state reset complete. updated_rows=%s total_rows=%s",
                result.get("updated_rows", 0),
                result.get("total_rows", 0),
            )
        except Exception as e:
            _log_runtime_exception("daily_state_reset_loop", e, event=None)


def _resolve_keepalive_url() -> str:
    raw = (config.HOURLY_KEEPALIVE_URL or config.PUBLIC_BASE_URL or "").strip()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"
    return raw.rstrip("/") + "/"


def _hourly_keepalive_loop():
    interval_seconds = max(60, int(config.HOURLY_KEEPALIVE_INTERVAL_MINUTES) * 60)
    keepalive_url = _resolve_keepalive_url()
    while not _scheduler_stop_event.is_set():
        if _scheduler_stop_event.wait(interval_seconds):
            return
        try:
            if keepalive_url:
                response = httpx.get(keepalive_url, timeout=10.0)
                logger.info(
                    "Keepalive ping done. url=%s status=%s",
                    keepalive_url,
                    response.status_code,
                )
            else:
                logger.info("Keepalive tick executed (no keepalive URL configured).")
        except Exception as e:
            _log_runtime_exception("hourly_keepalive_loop", e, event=None)


def _start_background_threads():
    global _daily_scheduler_thread, _hourly_scheduler_thread
    _scheduler_stop_event.clear()

    if config.DAILY_STATE_RESET_ENABLED and _daily_scheduler_thread is None:
        _daily_scheduler_thread = threading.Thread(target=_daily_state_reset_loop, daemon=True)
        _daily_scheduler_thread.start()
        logger.info(
            "Daily state reset scheduler started at %02d:%02d (Asia/Taipei)",
            int(config.DAILY_STATE_RESET_HOUR),
            int(config.DAILY_STATE_RESET_MINUTE),
        )

    if config.HOURLY_KEEPALIVE_ENABLED and _hourly_scheduler_thread is None:
        _hourly_scheduler_thread = threading.Thread(target=_hourly_keepalive_loop, daemon=True)
        _hourly_scheduler_thread.start()
        logger.info(
            "Keepalive scheduler started. interval_minutes=%d keepalive_url=%s",
            int(config.HOURLY_KEEPALIVE_INTERVAL_MINUTES),
            _resolve_keepalive_url() or "(not set)",
        )


def _stop_background_threads():
    global _daily_scheduler_thread, _hourly_scheduler_thread
    _scheduler_stop_event.set()

    if _daily_scheduler_thread is not None:
        _daily_scheduler_thread.join(timeout=5)
        _daily_scheduler_thread = None

    if _hourly_scheduler_thread is not None:
        _hourly_scheduler_thread.join(timeout=5)
        _hourly_scheduler_thread = None


def _load_temp_data_from_state(state) -> dict:
    raw = getattr(state, "temp_data", None)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


@app.get("/")
async def root():
    return {"status": "ok", "message": "Bartending Club Finance Agent is running"}


@app.post("/webhook")
async def webhook(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    if not signature:
        raise HTTPException(status_code=400, detail="Missing X-Line-Signature header")

    body = await request.body()
    body_decoded = body.decode("utf-8")
    if not _is_valid_line_signature(body_decoded, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    try:
        handler.handle(body_decoded, signature)
        return JSONResponse(content={"status": "ok"})
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")
    except Exception as e:
        _log_runtime_exception("webhook_handle_sync", e, event=None)
        raise HTTPException(status_code=500, detail="Webhook handler error")


def get_line_service() -> LineService:
    global _line_service
    if _line_service is None:
        _line_service = LineService()
    return _line_service


def get_services():
    global _sheets_service, _line_service, _state_manager
    line_service = get_line_service()
    if _sheets_service is None:
        _sheets_service = SheetsService()
        set_token_logger(_sheets_service.log_token_usage)
        _state_manager = StateManager(_sheets_service)
    return _sheets_service, line_service, _state_manager


@handler.add(MessageEvent, message=TextMessageContent)
def handle_text_message_sync(event: MessageEvent):
    try:
        handle_text_message(event)
    except Exception as e:
        _handle_event_exception("handle_text_message_sync", event, e, notify_user=True)


@handler.add(MessageEvent, message=ImageMessageContent)
def handle_image_message_sync(event: MessageEvent):
    try:
        handle_image_message(event)
    except Exception as e:
        _handle_event_exception("handle_image_message_sync", event, e, notify_user=True)


@handler.add(FollowEvent)
def handle_follow_event_sync(event: FollowEvent):
    try:
        handle_follow_event(event)
    except Exception as e:
        _handle_event_exception("handle_follow_event_sync", event, e, notify_user=True)


@handler.add(MessageEvent)
def handle_any_message_sync(event: MessageEvent):
    if isinstance(event.message, (TextMessageContent, ImageMessageContent)):
        return

    try:
        _, line_service, state_manager = get_services()
        user_id = getattr(event.source, "user_id", None)
        if not user_id:
            return

        try:
            profile = line_service.messaging_api.get_profile(user_id)
            display_name = profile.display_name or "Unknown"
        except Exception:
            display_name = "Unknown"

        if state_manager.user_exists(user_id):
            current_state = state_manager.get_state(user_id)
            state_manager.touch_user(user_id, display_name, current_state=current_state)
        else:
            _reply_activation_guide(line_service, event.reply_token, display_name)
    except Exception as e:
        _handle_event_exception("handle_any_message_sync", event, e, notify_user=True)


def _is_token(text: str, token_set: set[str]) -> bool:
    return text.strip().lower() in {t.lower() for t in token_set}


def _today_str() -> str:
    return datetime.now(_tw_timezone).strftime("%Y-%m-%d")


def _default_manual_record() -> dict:
    return {
        "_mode": "manual_bookkeeping",
        "date": _today_str(),
        "receipt_type": "無",
        "item_name": "",
        "category": "日常開銷",
        "amount": 0,
    }


def _is_manual_mode(temp_data: dict) -> bool:
    return isinstance(temp_data, dict) and temp_data.get("_mode") == "manual_bookkeeping"


def _extract_bookkeep_payload(text: str) -> tuple[bool, str]:
    raw = (text or "").strip()
    lower = raw.lower()
    for token in BOOKKEEP_TOKENS:
        if lower.startswith(token.lower()):
            payload = raw[len(token):].lstrip(":：").strip()
            return True, payload
    return False, ""


def _preserve_meta_fields(old_data: dict, new_invoice: InvoiceData) -> dict:
    merged = new_invoice.model_dump()
    if "_image_message_id" in old_data:
        merged["_image_message_id"] = old_data["_image_message_id"]
    return merged


def _looks_like_invoice_data(invoice_data: InvoiceData) -> bool:
    has_amount = (invoice_data.amount or 0) > 0
    has_date = bool((invoice_data.date or "").strip()) and invoice_data.date != "1970-01-01"
    invoice_type = (invoice_data.invoice_type or "").strip()
    is_supported_type = invoice_type in {"發票", "收據", "空白收據"}

    if not is_supported_type:
        return False

    # Do not require item rows; some receipts only expose total/date/type clearly.
    return has_amount and has_date


def _eligibility_text(eligibility: int) -> str:
    if eligibility == 1:
        return "資料齊全且金額夠"
    if eligibility == 2:
        return "資料齊全但金額小"
    return "不符合"


def _reply_edit_guide(line_service: LineService, reply_token: str):
    msg = (
        "請直接輸入你要修改的內容，我會自動判斷欄位並更新後再給你確認卡片。\n"
        "可修改：日期、金額、賣方統編、買方統編、品項、發票類型、消費類別。\n"
        "範例：\n"
        "1) 日期改成 2026-03-15\n"
        "2) 金額改 580\n"
        "3) 賣方統編改 12345678\n"
        "4) 類型改收據\n"
        "5) 類別改日常開銷與練習、長期硬體設備購置、社課開銷、活動開銷"
    )
    line_service.reply_text(reply_token, msg)


def _reply_manual_edit_guide(line_service: LineService, reply_token: str):
    msg = (
        "請隨便輸入記帳內容，我會用 LLM 解析成卡片。\n"
        "收據可用：空白收據 / 收據 / 發票 / 無\n"
        "分類可用：日常開銷 / 設備購置 / 社課開銷 / 活動開銷\n\n"
        "請提供明確資訊，尤其是金額、日期、品項與分類。\n\n"
        "範例：\n"
        "1) 3/15 社課材料 檸檬汁 320 收據\n"
        "2) 今天 塑膠杯 活動開銷 1200 發票\n"
        "3) 昨天 量筒 設備 200"
    )
    line_service.reply_text(reply_token, msg)


def _activation_guide_text(display_name: str) -> str:
    name = (display_name or "").strip() or "同學"
    return (
        f"{name} 你好，歡迎使用記帳機器人。\n"
        "使用方式：\n"
        "1) 傳「記帳」開始手動記帳\n"
        "2) 或直接上傳發票/收據照片\n"
        "3) 依提示「確認 / 修改 / 取消」\n\n"
        "請先回覆一個「好」完成啟用。\n"
        "備註：若忘記如何使用，可隨時輸入「如何使用」。"
    )


def _is_activation_token(text: str) -> bool:
    return (text or "").strip() == ACTIVATION_TOKEN


def _is_usage_guide_token(text: str) -> bool:
    raw = (text or "").strip()
    return raw in USAGE_GUIDE_TOKENS


def _compact_text(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "")).strip()


def _is_admin_user(user_id: str) -> bool:
    return bool(user_id) and user_id in config.ADMIN_LINE_IDS


def _is_finance_guide_request(text: str) -> bool:
    compact = _compact_text(text)
    if not compact:
        return False
    if any(keyword in compact for keyword in FINANCE_GUIDE_KEYWORDS):
        return True
    return "財務" in compact and any(k in compact for k in {"教學", "使用", "手冊"})


def _parse_admin_command(text: str) -> tuple[str, str]:
    raw = (text or "").strip()
    compact = _compact_text(raw)
    if compact in {"執行配對", "財務執行配對"}:
        return "run_match", ""
    if compact in {"重新執行配對", "重新配對", "財務重新執行配對", "財務重新配對"}:
        return "rematch", ""
    if compact in {"補助總覽", "財務補助總覽", "核銷總覽", "財務核銷總覽"}:
        return "overview", ""

    item_m = re.match(r"^(?:財務)?補助品項查詢\s*[:：]?\s*(.+)$", raw)
    if item_m:
        return "activity_items", item_m.group(1).strip()

    gap_m = re.match(r"^(?:財務)?核銷(?:夠不夠用|夠不夠|狀況)\s*[:：]?\s*(.*)$", raw)
    if gap_m:
        return "gap", gap_m.group(1).strip()

    return "", ""


def _finance_guide_text(is_admin: bool) -> str:
    base = (
        "財務操作手冊\n"
        "一般流程：\n"
        "1) 發票先登錄，不會自動配對補助\n"
        "2) 財務人員再手動執行配對\n"
        "3) 需要重算可用「重新執行配對」\n\n"
        "財務指令：\n"
        "1) 執行配對\n"
        "2) 重新執行配對\n"
        "3) 補助總覽\n"
        "4) 補助品項查詢 <活動ID>\n"
        "5) 核銷夠不夠用 <活動ID>\n\n"
        "範例：\n"
        "補助品項查詢 114001\n"
        "核銷夠不夠用 114001"
    )
    if is_admin:
        return base
    return base + "\n\n你目前不是財務/admin 身分，僅可查看教學。"


def _format_matching_result(result: dict) -> str:
    mode = "重新執行配對" if result.get("rematch") else "執行配對"
    msg = (
        f"{mode}完成\n"
        f"處理筆數: {result.get('processed', 0)}\n"
        f"成功配對: {result.get('matched', 0)}\n"
        f"未配對: {result.get('unmatched', 0)}\n"
        f"略過: {result.get('skipped', 0)}"
    )
    if result.get("rematch"):
        msg += (
            f"\n已重設補助列數: {result.get('subsidy_reset_rows', 0)}"
            f"\n已清空發票配對列數: {result.get('cleared_invoice_rows', 0)}"
        )
    preview = result.get("unmatched_preview") or []
    if preview:
        msg += f"\n未配對發票(前{len(preview)}筆): " + ", ".join(preview)
    return msg


def _format_subsidy_overview_text(rows: list[dict]) -> str:
    if not rows:
        return "查無補助資料。"
    lines = ["補助總覽："]
    for item in rows[:20]:
        status = "足夠" if item.get("is_enough") else "不足"
        lines.append(
            f"- {item['activity_id']} {item['activity_name']} | "
            f"補助 {int(item['subsidy_amount'])} / 已核銷 {int(item['current_accumulated'])} / "
            f"缺口 {int(item['gap'])} ({status})"
        )
    if len(rows) > 20:
        lines.append(f"...其餘 {len(rows) - 20} 筆請分批查詢")
    return "\n".join(lines)


def _format_activity_reconciliation_text(result: dict) -> str:
    if not result.get("found"):
        return f"找不到活動ID：{result.get('activity_id') or '(空白)'}"
    lines = [
        f"活動 {result['activity_id']} {result.get('activity_name', '')}",
        f"補助額度: {int(result.get('subsidy_amount') or 0)}",
        f"已核銷: {int(result.get('current_accumulated') or 0)}",
        f"缺口: {int(result.get('gap') or 0)}",
        f"已配對發票數: {result.get('matched_invoice_count', 0)}",
        f"已配對總金額: {int(result.get('matched_total_amount') or 0)}",
    ]
    items = result.get("items") or []
    if items:
        lines.append("配對品項(前20筆)：")
        for it in items[:20]:
            lines.append(f"- {it['invoice_id']} | {it['invoice_date']} | {it['item_name']} | ${it['amount']}")
    else:
        lines.append("目前無已配對發票。")
    return "\n".join(lines)


def _handle_admin_command(
    user_id: str,
    display_name: str,
    reply_token: str,
    text: str,
    sheets_service: SheetsService,
    line_service: LineService,
) -> bool:
    cmd, arg = _parse_admin_command(text)
    if not cmd:
        return False

    if cmd == "run_match":
        result = sheets_service.run_invoice_matching(rematch=False, user=user_id)
        line_service.reply_text(reply_token, _format_matching_result(result))
        return True

    if cmd == "rematch":
        result = sheets_service.run_invoice_matching(rematch=True, user=user_id)
        line_service.reply_text(reply_token, _format_matching_result(result))
        return True

    if cmd == "overview":
        rows = sheets_service.get_subsidy_overview()
        line_service.reply_text(reply_token, _format_subsidy_overview_text(rows))
        return True

    if cmd == "activity_items":
        if not arg:
            line_service.reply_text(reply_token, "請輸入活動ID，例如：補助品項查詢 114001")
            return True
        result = sheets_service.get_activity_reconciliation(arg, limit=20)
        line_service.reply_text(reply_token, _format_activity_reconciliation_text(result))
        return True

    if cmd == "gap":
        if not arg:
            rows = sheets_service.get_subsidy_overview()
            line_service.reply_text(reply_token, _format_subsidy_overview_text(rows))
            return True
        status = sheets_service.get_activity_gap_status(arg)
        if not status:
            line_service.reply_text(reply_token, f"找不到活動ID：{arg}")
            return True
        status_text = "足夠" if status.get("is_enough") else "不足"
        line_service.reply_text(
            reply_token,
            (
                f"活動 {status['activity_id']} {status['activity_name']}\n"
                f"補助額度: {int(status['subsidy_amount'])}\n"
                f"已核銷: {int(status['current_accumulated'])}\n"
                f"缺口: {int(status['gap'])}\n"
                f"狀態: {status_text}"
            ),
        )
        return True

    return False


def _reply_activation_guide(line_service: LineService, reply_token: str, display_name: str):
    line_service.reply_flex(
        reply_token,
        "歡迎使用",
        _build_activation_guide_flex(display_name),
    )


def _build_activation_guide_flex(display_name: str) -> dict:
    name = (display_name or "").strip() or "同學"
    return {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {
                    "type": "text",
                    "text": f"{name} 歡迎使用",
                    "weight": "bold",
                    "size": "lg",
                    "wrap": True,
                },
                {
                    "type": "text",
                    "text": "使用方式：\n1) 傳「記帳」開始手動記帳\n2) 或直接上傳發票/收據照片\n3) 依提示「確認 / 修改 / 取消」",
                    "size": "sm",
                    "color": "#555555",
                    "wrap": True,
                    "margin": "md",
                },
                {
                    "type": "text",
                    "text": "備註：忘記如何使用，可直接輸入「如何使用」。",
                    "size": "sm",
                    "color": "#777777",
                    "wrap": True,
                    "margin": "md",
                },
            ],
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {
                    "type": "button",
                    "style": "primary",
                    "height": "sm",
                    "action": {
                        "type": "message",
                        "label": "好，開始使用",
                        "text": "好",
                    },
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "height": "sm",
                    "action": {
                        "type": "message",
                        "label": "如何使用",
                        "text": "如何使用",
                    },
                },
            ],
            "flex": 0,
        },
    }


def _to_amount(value) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _strip_pending_amount_meta(data: dict) -> dict:
    if not isinstance(data, dict):
        return {}
    return {k: v for k, v in data.items() if not str(k).startswith("_pending_")}


def _is_amount_confirm_pending(data: dict) -> bool:
    return isinstance(data, dict) and data.get("_pending_amount_confirm") is True


def _should_confirm_amount_change(raw_text: str, old_amount: int, new_amount: int) -> bool:
    text = (raw_text or "").strip()
    is_numeric_only = bool(re.fullmatch(r"\d+", text))
    is_changed = int(new_amount) != int(old_amount)
    is_large = int(new_amount) > AMOUNT_ALERT_THRESHOLD
    return is_changed and (is_numeric_only or is_large)


def _build_amount_confirm_flex(old_amount: int, new_amount: int) -> dict:
    warning = ""
    if new_amount > AMOUNT_ALERT_THRESHOLD:
        warning = f"提醒：超過 {AMOUNT_ALERT_THRESHOLD} 屬於大金額，請再次確認。"

    contents = [
        {
            "type": "text",
            "text": "金額修改確認",
            "weight": "bold",
            "size": "lg",
        },
        {
            "type": "text",
            "text": f"原金額：${old_amount}\n新金額：${new_amount}",
            "margin": "md",
            "wrap": True,
        },
        {
            "type": "text",
            "text": "是否要將金額改成這個數字？",
            "size": "sm",
            "color": "#555555",
            "margin": "md",
            "wrap": True,
        },
    ]
    if warning:
        contents.append(
            {
                "type": "text",
                "text": warning,
                "size": "sm",
                "color": "#B00020",
                "margin": "md",
                "wrap": True,
            }
        )

    return {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "contents": contents,
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {
                    "type": "button",
                    "style": "primary",
                    "height": "sm",
                    "action": {
                        "type": "message",
                        "label": "確認改金額",
                        "text": "確認改金額",
                    },
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "height": "sm",
                    "action": {
                        "type": "message",
                        "label": "先不要改",
                        "text": "不要改金額",
                    },
                },
            ],
            "flex": 0,
        },
    }


def _make_pending_amount_payload(base_data: dict, candidate_data: dict, old_amount: int, new_amount: int) -> dict:
    return {
        **_strip_pending_amount_meta(base_data),
        "_pending_amount_confirm": True,
        "_pending_old_amount": int(old_amount),
        "_pending_new_amount": int(new_amount),
        "_pending_base_data": _strip_pending_amount_meta(base_data),
        "_pending_candidate_data": _strip_pending_amount_meta(candidate_data),
    }


def _is_amount_confirm_accept(text: str) -> bool:
    return (text or "").strip() in AMOUNT_CONFIRM_ACCEPT_TOKENS


def _is_amount_confirm_reject(text: str) -> bool:
    return (text or "").strip() in AMOUNT_CONFIRM_REJECT_TOKENS


def handle_follow_event(event: FollowEvent):
    _, line_service, state_manager = get_services()
    user_id = event.source.user_id
    reply_token = event.reply_token

    try:
        profile = line_service.messaging_api.get_profile(user_id)
        display_name = profile.display_name or "同學"
    except Exception:
        display_name = "同學"

    if state_manager.user_exists(user_id):
        current_state = state_manager.get_state(user_id)
        state_manager.touch_user(user_id, display_name, current_state=current_state)

    _reply_activation_guide(line_service, reply_token, display_name)


def _apply_edit_and_reply(user_id: str, reply_token: str, text: str):
    _, line_service, state_manager = get_services()
    temp_data = _strip_pending_amount_meta(state_manager.get_temp_data(user_id))
    if not temp_data:
        line_service.reply_text(reply_token, "目前沒有可修改資料，請先上傳一張發票圖片。")
        return

    trace_id = f"{user_id}:{reply_token}"
    updated_invoice = apply_user_edit(temp_data, text, trace_id=trace_id)
    merged_data = _preserve_meta_fields(temp_data, updated_invoice)

    old_amount = _to_amount(temp_data.get("amount"))
    new_amount = _to_amount(merged_data.get("amount"))
    if _should_confirm_amount_change(text, old_amount, new_amount):
        pending_payload = _make_pending_amount_payload(temp_data, merged_data, old_amount, new_amount)
        state_manager.set_state(user_id, AppState.WAITING_FOR_INFO, pending_payload)
        line_service.reply_flex(
            reply_token,
            "請確認是否修改金額",
            _build_amount_confirm_flex(old_amount, new_amount),
        )
        return

    state_manager.set_state(user_id, AppState.WAITING_FOR_CONFIRM, merged_data)
    flex_message = line_service.build_confirmation_flex(merged_data)
    line_service.reply_flex(reply_token, "已更新內容，請再次確認", flex_message)


def _apply_manual_parse_and_reply(user_id: str, reply_token: str, text: str):
    _, line_service, state_manager = get_services()
    temp_data = _strip_pending_amount_meta(state_manager.get_temp_data(user_id))
    if not _is_manual_mode(temp_data):
        temp_data = _default_manual_record()

    trace_id = f"{user_id}:{reply_token}"
    parsed = parse_manual_record_text(text, temp_data, trace_id=trace_id)
    parsed["_mode"] = "manual_bookkeeping"

    old_amount = _to_amount(temp_data.get("amount"))
    new_amount = _to_amount(parsed.get("amount"))
    if _should_confirm_amount_change(text, old_amount, new_amount):
        pending_payload = _make_pending_amount_payload(temp_data, parsed, old_amount, new_amount)
        state_manager.set_state(user_id, AppState.WAITING_FOR_INFO, pending_payload)
        line_service.reply_flex(
            reply_token,
            "請確認是否修改金額",
            _build_amount_confirm_flex(old_amount, new_amount),
        )
        return

    changed = []
    for k in ["date", "receipt_type", "item_name", "category", "amount"]:
        if temp_data.get(k) != parsed.get(k):
            changed.append(k)

    state_manager.set_state(user_id, AppState.WAITING_FOR_CONFIRM, parsed)
    flex = line_service.build_manual_record_flex(parsed)
    if changed:
        line_service.reply_flex(reply_token, f"已更新：{', '.join(changed)}", flex)
    else:
        line_service.reply_flex(reply_token, "已解析完成，請確認", flex)


def handle_text_message(event: MessageEvent):
    sheets_service, line_service, state_manager = get_services()

    user_id = event.source.user_id
    text = event.message.text.strip()
    reply_token = event.reply_token

    try:
        profile = line_service.messaging_api.get_profile(user_id)
        display_name = profile.display_name
    except Exception:
        display_name = "Unknown"

    is_known_user = state_manager.user_exists(user_id)
    is_admin = _is_admin_user(user_id)

    if _is_finance_guide_request(text):
        if is_known_user:
            guide_state = state_manager.get_state(user_id)
            state_manager.touch_user(user_id, display_name, current_state=guide_state)
        elif is_admin:
            state_manager.touch_user(user_id, display_name)
        line_service.reply_text(reply_token, _finance_guide_text(is_admin=is_admin))
        return

    admin_cmd, _ = _parse_admin_command(text)
    if admin_cmd:
        if not is_admin:
            line_service.reply_text(reply_token, "這個功能僅限財務/admin 帳號使用。")
            return
        if not is_known_user:
            state_manager.touch_user(user_id, display_name)
            is_known_user = True
        admin_state = state_manager.get_state(user_id)
        state_manager.touch_user(user_id, display_name, current_state=admin_state)
        handled = _handle_admin_command(
            user_id=user_id,
            display_name=display_name,
            reply_token=reply_token,
            text=text,
            sheets_service=sheets_service,
            line_service=line_service,
        )
        if handled:
            return

    if not is_known_user:
        if _is_activation_token(text):
            state_manager.touch_user(user_id, display_name)
            line_service.reply_text(
                reply_token,
                "已完成啟用。你可以傳「記帳」開始手動記帳，或直接上傳發票圖片。",
            )
        elif _is_usage_guide_token(text):
            _reply_activation_guide(line_service, reply_token, display_name)
        else:
            _reply_activation_guide(line_service, reply_token, display_name)
        return

    state = state_manager.get_state(user_id)
    state_manager.touch_user(user_id, display_name, current_state=state)

    if _is_usage_guide_token(text):
        _reply_activation_guide(line_service, reply_token, display_name)
        return

    # cancel works globally
    if _is_token(text, CANCEL_TOKENS):
        state_manager.clear_state(user_id, user_name=display_name, current_state=state)
        line_service.reply_text(reply_token, "已取消目前流程。")
        return

    # manual bookkeeping entry point
    is_bookkeep, payload = _extract_bookkeep_payload(text)
    if is_bookkeep:
        if payload:
            base = _default_manual_record()
            state_manager.set_state(user_id, AppState.WAITING_FOR_INFO, base)
            _apply_manual_parse_and_reply(user_id, reply_token, payload)
            return

        base = _default_manual_record()
        state_manager.set_state(user_id, AppState.WAITING_FOR_INFO, base)
        _reply_manual_edit_guide(line_service, reply_token)
        return

    temp_data = _load_temp_data_from_state(state)

    if _is_amount_confirm_pending(temp_data):
        old_amount = _to_amount(temp_data.get("_pending_old_amount"))
        new_amount = _to_amount(temp_data.get("_pending_new_amount"))

        if _is_amount_confirm_accept(text):
            candidate_data = _strip_pending_amount_meta(temp_data.get("_pending_candidate_data") or {})
            if not candidate_data:
                candidate_data = _strip_pending_amount_meta(temp_data)
            state_manager.set_state(user_id, AppState.WAITING_FOR_CONFIRM, candidate_data)
            if _is_manual_mode(candidate_data):
                flex = line_service.build_manual_record_flex(candidate_data)
            else:
                flex = line_service.build_confirmation_flex(candidate_data)
            line_service.reply_flex(reply_token, "金額已更新，請確認資料", flex)
            return

        if _is_amount_confirm_reject(text):
            base_data = _strip_pending_amount_meta(temp_data.get("_pending_base_data") or {})
            if base_data:
                state_manager.set_state(user_id, AppState.WAITING_FOR_INFO, base_data)
            else:
                state_manager.clear_state(user_id)
            line_service.reply_text(reply_token, "已取消本次金額修改，請繼續輸入你要修改的內容。")
            return

        line_service.reply_flex(
            reply_token,
            "請先確認金額是否要修改",
            _build_amount_confirm_flex(old_amount, new_amount),
        )
        return

    if state.state == AppState.WAITING_FOR_CONFIRM.value:
        if _is_manual_mode(temp_data):
            if _is_token(text, CONFIRM_TOKENS):
                amount = int(temp_data.get("amount", 0) or 0)
                if amount <= 0:
                    line_service.reply_text(reply_token, "總金額需大於 0，請再補充金額。")
                    return

                result = sheets_service.save_manual_record(
                    user_id=user_id,
                    display_name=display_name,
                    record_date=temp_data.get("date", _today_str()),
                    receipt_type=temp_data.get("receipt_type", "無"),
                    item_name=temp_data.get("item_name", ""),
                    category=temp_data.get("category", "日常開銷"),
                    amount=amount,
                )
                state_manager.clear_state(user_id)
                line_service.reply_text(
                    reply_token,
                    f"已完成手動記帳\n流水號: {result['invoice_id']}\n核銷可用性: {_eligibility_text(result['eligibility'])}",
                )
                return

            if _is_token(text, EDIT_TOKENS):
                state_manager.set_state(user_id, AppState.WAITING_FOR_INFO, temp_data)
                _reply_manual_edit_guide(line_service, reply_token)
                return

            # In manual confirm state, any non-command text will be re-parsed by LLM.
            state_manager.set_state(user_id, AppState.WAITING_FOR_INFO, temp_data)
            _apply_manual_parse_and_reply(user_id, reply_token, text)
            return

        # invoice confirm flow
        if _is_token(text, CONFIRM_TOKENS):
            invoice_data = InvoiceData(**{k: v for k, v in temp_data.items() if not k.startswith("_")})
            raw_message_id = str(temp_data.get("_image_message_id", "")).strip()
            image_url = f"LINE_MESSAGE_ID:{raw_message_id}" if raw_message_id else "都沒有"

            result = sheets_service.save_invoice_and_match(
                user_id,
                display_name,
                invoice_data,
                image_url,
                auto_match=False,
            )
            state_manager.clear_state(user_id)

            eligibility_text = _eligibility_text(result["eligibility"])
            reply_msg = (
                f"已完成登錄\n"
                f"發票ID: {result['invoice_id']}\n"
                f"核銷可用性: {eligibility_text}"
            )
            if result["matched_activity"]:
                reply_msg += f"\n已配對活動: {result['matched_activity']}"
            else:
                reply_msg += "\n配對狀態: 待財務執行配對"

            line_service.reply_text(reply_token, reply_msg)
            return

        if _is_token(text, EDIT_TOKENS):
            state_manager.set_state(user_id, AppState.WAITING_FOR_INFO, temp_data)
            _reply_edit_guide(line_service, reply_token)
            return

        _apply_edit_and_reply(user_id, reply_token, text)
        return

    if state.state == AppState.WAITING_FOR_INFO.value:
        if not temp_data:
            line_service.reply_text(reply_token, "目前沒有可修改資料，請先上傳發票圖片或輸入記帳。")
            return

        if _is_manual_mode(temp_data):
            if _is_token(text, CONFIRM_TOKENS):
                state_manager.set_state(user_id, AppState.WAITING_FOR_CONFIRM, temp_data)
                flex = line_service.build_manual_record_flex(temp_data)
                line_service.reply_flex(reply_token, "以下是目前手動記帳資料，請確認", flex)
                return

            if _is_token(text, SKIP_TOKENS):
                state_manager.set_state(user_id, AppState.WAITING_FOR_CONFIRM, temp_data)
                flex = line_service.build_manual_record_flex(temp_data)
                line_service.reply_flex(reply_token, "以下是目前手動記帳資料，請確認", flex)
                return

            _apply_manual_parse_and_reply(user_id, reply_token, text)
            return

        if _is_token(text, CONFIRM_TOKENS):
            state_manager.set_state(user_id, AppState.WAITING_FOR_CONFIRM, temp_data)
            flex_message = line_service.build_confirmation_flex(temp_data)
            line_service.reply_flex(reply_token, "以下是目前資料，請確認", flex_message)
            return

        if _is_token(text, SKIP_TOKENS):
            state_manager.set_state(user_id, AppState.WAITING_FOR_CONFIRM, temp_data)
            flex_message = line_service.build_confirmation_flex(temp_data)
            line_service.reply_flex(reply_token, "以下是目前資料，請確認", flex_message)
            return

        _apply_edit_and_reply(user_id, reply_token, text)
        return

    line_service.reply_text(reply_token, "請先上傳發票圖片，或輸入「記帳」開始手動記帳。")


def handle_image_message(event: MessageEvent):
    _, line_service, state_manager = get_services()

    user_id = event.source.user_id
    message_id = event.message.id
    reply_token = event.reply_token

    try:
        try:
            profile = line_service.messaging_api.get_profile(user_id)
            display_name = profile.display_name or "同學"
        except Exception:
            display_name = "同學"

        if not state_manager.user_exists(user_id):
            _reply_activation_guide(line_service, reply_token, display_name)
            return

        current_state = state_manager.get_state(user_id)
        state_manager.touch_user(user_id, display_name, current_state=current_state)
        state_manager.clear_state(user_id, user_name=display_name, current_state=current_state)

        line_service.reply_text(reply_token, f"{display_name} 你好，已接收到照片，正在處理中。")

        image_content = line_service.get_message_content(message_id)
        trace_id = f"{user_id}:{message_id}"
        invoice_data = extract_invoice_data(image_content, trace_id=trace_id)

        if not _looks_like_invoice_data(invoice_data):
            line_service.push_text(
                user_id,
                "非發票相關內容 無法使用",
            )
            return

        temp_data = invoice_data.model_dump()
        temp_data["_image_message_id"] = message_id

        state_manager.set_state(user_id, AppState.WAITING_FOR_CONFIRM, temp_data)

        flex_message = line_service.build_confirmation_flex(temp_data)
        line_service.push_flex(user_id, "辨識完成，請先確認資料", flex_message)

    except Exception as e:
        _log_runtime_exception("handle_image_message", e, event=event)
        line_service.push_text(user_id, "處理圖片失敗，請稍後再試一次。")
