from datetime import datetime

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent, ImageMessageContent

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

app = FastAPI(title="Bartending Club Finance Agent")
handler = WebhookHandler(config.LINE_CHANNEL_SECRET)

CONFIRM_TOKENS = {"確認", "确认", "confirm", "ok", "yes"}
EDIT_TOKENS = {"修改", "更正", "edit", "fix"}
CANCEL_TOKENS = {"取消", "cancel", "reset"}
SKIP_TOKENS = {"略過", "跳過", "skip"}
BOOKKEEP_TOKENS = {"記帳", "記賬", "手動記帳", "manual"}

# Global service instances
_sheets_service = None
_line_service = None
_state_manager = None


@app.get("/")
async def root():
    return {"status": "ok", "message": "Bartending Club Finance Agent is running"}


@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    signature = request.headers.get("X-Line-Signature", "")
    if not signature:
        raise HTTPException(status_code=400, detail="Missing X-Line-Signature header")

    body = await request.body()
    body_decoded = body.decode("utf-8")

    try:
        background_tasks.add_task(handler.handle, body_decoded, signature)
        return JSONResponse(content={"status": "ok"})
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")
    except Exception as e:
        print(f"Error scheduling background task: {e}")
        return JSONResponse(content={"status": "error", "detail": str(e)})


def get_services():
    global _sheets_service, _line_service, _state_manager
    if _sheets_service is None:
        _sheets_service = SheetsService()
        set_token_logger(_sheets_service.log_token_usage)
        _line_service = LineService()
        _state_manager = StateManager(_sheets_service)
    return _sheets_service, _line_service, _state_manager


@handler.add(MessageEvent, message=TextMessageContent)
def handle_text_message_sync(event: MessageEvent):
    try:
        handle_text_message(event)
    except Exception:
        import traceback

        with open("debug_error.log", "a", encoding="utf-8") as f:
            traceback.print_exc(file=f)


@handler.add(MessageEvent, message=ImageMessageContent)
def handle_image_message_sync(event: MessageEvent):
    try:
        handle_image_message(event)
    except Exception:
        import traceback

        with open("debug_error.log", "a", encoding="utf-8") as f:
            traceback.print_exc(file=f)


def _is_token(text: str, token_set: set[str]) -> bool:
    return text.strip().lower() in {t.lower() for t in token_set}


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


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
            payload = raw[len(token):].strip().lstrip(":：")
            return True, payload
    return False, ""


def _preserve_meta_fields(old_data: dict, new_invoice: InvoiceData) -> dict:
    merged = new_invoice.model_dump()
    if "_image_message_id" in old_data:
        merged["_image_message_id"] = old_data["_image_message_id"]
    return merged


def _looks_like_invoice_data(invoice_data: InvoiceData) -> bool:
    def _valid_tax_id(value: str) -> bool:
        return isinstance(value, str) and value.isdigit() and len(value) == 8

    has_amount = (invoice_data.amount or 0) > 0
    has_items = any(bool((item.name or "").strip()) for item in (invoice_data.items or []))
    has_vendor = _valid_tax_id((invoice_data.vendor_tax_id or "").strip())
    has_buyer = _valid_tax_id((invoice_data.buyer_tax_id or "").strip())
    has_date = bool((invoice_data.date or "").strip()) and invoice_data.date != "1970-01-01"
    invoice_type = (invoice_data.invoice_type or "").strip()
    is_supported_type = invoice_type in {"發票", "收據", "空白收據"}

    if not is_supported_type:
        return False

    # Blank receipts usually do not have tax IDs, but should still have basic structure.
    if invoice_type == "空白收據":
        return has_amount and has_date and has_items

    # Normal invoices/receipts must contain core structured fields.
    return has_amount and has_date and has_items


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


def _apply_edit_and_reply(user_id: str, reply_token: str, text: str):
    _, line_service, state_manager = get_services()
    temp_data = state_manager.get_temp_data(user_id)
    if not temp_data:
        line_service.reply_text(reply_token, "目前沒有可修改資料，請先上傳一張發票圖片。")
        return

    trace_id = f"{user_id}:{reply_token}"
    updated_invoice = apply_user_edit(temp_data, text, trace_id=trace_id)
    merged_data = _preserve_meta_fields(temp_data, updated_invoice)

    state_manager.set_state(user_id, AppState.WAITING_FOR_CONFIRM, merged_data)
    flex_message = line_service.build_confirmation_flex(merged_data)
    line_service.reply_flex(reply_token, "已更新內容，請再次確認", flex_message)


def _apply_manual_parse_and_reply(user_id: str, reply_token: str, text: str):
    _, line_service, state_manager = get_services()
    temp_data = state_manager.get_temp_data(user_id)
    if not _is_manual_mode(temp_data):
        temp_data = _default_manual_record()

    trace_id = f"{user_id}:{reply_token}"
    parsed = parse_manual_record_text(text, temp_data, trace_id=trace_id)
    parsed["_mode"] = "manual_bookkeeping"

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

    # cancel works globally
    if _is_token(text, CANCEL_TOKENS):
        state_manager.clear_state(user_id)
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

    state = state_manager.get_state(user_id)

    if state.state == AppState.WAITING_FOR_CONFIRM.value:
        temp_data = state_manager.get_temp_data(user_id)

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

            result = sheets_service.save_invoice_and_match(user_id, display_name, invoice_data, image_url)
            state_manager.clear_state(user_id)

            eligibility_text = _eligibility_text(result["eligibility"])
            reply_msg = (
                f"已完成登錄\n"
                f"發票ID: {result['invoice_id']}\n"
                f"核銷可用性: {eligibility_text}"
            )
            if result["matched_activity"]:
                reply_msg += f"\n已配對活動: {result['matched_activity']}"

            line_service.reply_text(reply_token, reply_msg)
            return

        if _is_token(text, EDIT_TOKENS):
            state_manager.set_state(user_id, AppState.WAITING_FOR_INFO, temp_data)
            _reply_edit_guide(line_service, reply_token)
            return

        _apply_edit_and_reply(user_id, reply_token, text)
        return

    if state.state == AppState.WAITING_FOR_INFO.value:
        temp_data = state_manager.get_temp_data(user_id)
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

    state_manager.clear_state(user_id)

    try:
        try:
            profile = line_service.messaging_api.get_profile(user_id)
            display_name = profile.display_name or "同學"
        except Exception:
            display_name = "同學"

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
        print(f"Error processing image: {e}")
        line_service.push_text(user_id, "處理圖片失敗，請稍後再試一次。")
