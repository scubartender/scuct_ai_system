import base64
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from openai import OpenAI

from config import config
from core.schemas import InvoiceData

# Initialize OpenAI client
client = OpenAI(api_key=config.OPENAI_API_KEY)
logger = logging.getLogger(__name__)
_token_logger: Optional[Callable[[str, int, str], None]] = None
_tw_timezone = timezone(timedelta(hours=8))

EDITABLE_FIELDS = [
    "date",
    "amount",
    "vendor_tax_id",
    "buyer_tax_id",
    "items",
    "invoice_type",
    "consumption_category",
]


def set_token_logger(logger: Optional[Callable[[str, int, str], None]]):
    global _token_logger
    _token_logger = logger


def _safe_int(value) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _extract_total_tokens(usage) -> int:
    if usage is None:
        return 0

    if isinstance(usage, dict):
        total = usage.get("total_tokens")
        if total is not None:
            return max(0, _safe_int(total))
        prompt_tokens = usage.get("prompt_tokens", usage.get("input_tokens", 0))
        completion_tokens = usage.get("completion_tokens", usage.get("output_tokens", 0))
        return max(0, _safe_int(prompt_tokens) + _safe_int(completion_tokens))

    total = getattr(usage, "total_tokens", None)
    if total is not None:
        return max(0, _safe_int(total))

    prompt_tokens = getattr(usage, "prompt_tokens", getattr(usage, "input_tokens", 0))
    completion_tokens = getattr(usage, "completion_tokens", getattr(usage, "output_tokens", 0))
    return max(0, _safe_int(prompt_tokens) + _safe_int(completion_tokens))


def _emit_token_log(action: str, token: int, details: str = ""):
    if _token_logger is None:
        return
    try:
        _token_logger(action, max(0, _safe_int(token)), details)
    except Exception as e:
        logger.exception("Error logging token usage: %s", e)


def _sanitize_invoice_payload(data: dict) -> dict:
    return {k: data[k] for k in EDITABLE_FIELDS if k in data}


def _default_invoice_type() -> str:
    return InvoiceData.model_fields["invoice_type"].annotation.__args__[0]


def _default_consumption_category() -> str:
    return InvoiceData.model_fields["consumption_category"].annotation.__args__[0]


def _today_str() -> str:
    return datetime.now(_tw_timezone).strftime("%Y-%m-%d")


def _normalize_manual_data(data: dict) -> dict:
    out = dict(data or {})
    allowed_receipts = {"空白收據", "收據", "發票", "無"}
    allowed_categories = {"日常開銷", "設備購置", "社課開銷", "活動開銷"}

    out["date"] = out.get("date") or _today_str()
    out["item_name"] = str(out.get("item_name") or "").strip()
    out["amount"] = max(0, int(out.get("amount", 0) or 0))

    receipt = str(out.get("receipt_type") or "無").strip()
    if receipt not in allowed_receipts:
        receipt = "無"
    out["receipt_type"] = receipt

    category = str(out.get("category") or "日常開銷").strip()
    if category not in allowed_categories:
        category = "日常開銷"
    out["category"] = category

    m = re.search(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", out["date"])
    if m:
        y, mo, d = m.groups()
        out["date"] = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    else:
        out["date"] = _today_str()

    return out


def parse_manual_record_text(user_text: str, current_data: dict | None = None, trace_id: str = "") -> dict:
    """
    Parse free-form bookkeeping text into manual record fields.
    Fields: date, receipt_type, item_name, category, amount
    """
    base = _normalize_manual_data(
        current_data
        or {
            "date": _today_str(),
            "receipt_type": "無",
            "item_name": "",
            "category": "日常開銷",
            "amount": 0,
        }
    )

    prompt = (
        "You are parsing bookkeeping text into structured fields.\n"
        "Output JSON with keys: date, receipt_type, item_name, category, amount.\n"
        "Allowed receipt_type: 空白收據, 收據, 發票, 無\n"
        "Allowed category: 日常開銷, 設備購置, 社課開銷, 活動開銷\n"
        "Rules:\n"
        "1) Keep current values if user did not mention a field.\n"
        "2) date format must be YYYY-MM-DD.\n"
        "3) amount is integer >= 0.\n"
        "4) If unclear, keep current values.\n"
    )

    schema = {
        "type": "object",
        "properties": {
            "date": {"type": "string"},
            "receipt_type": {
                "type": "string",
                "enum": ["空白收據", "收據", "發票", "無"],
            },
            "item_name": {"type": "string"},
            "category": {
                "type": "string",
                "enum": ["日常開銷", "設備購置", "社課開銷", "活動開銷"],
            },
            "amount": {"type": "integer"},
        },
        "required": ["date", "receipt_type", "item_name", "category", "amount"],
        "additionalProperties": False,
    }

    try:
        completion = client.chat.completions.create(
            model=config.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": (
                        f"Current data:\n{base}\n\n"
                        f"User text:\n{user_text}"
                    ),
                },
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "manual_record",
                    "schema": schema,
                    "strict": True,
                },
            },
        )
        used_tokens = _extract_total_tokens(getattr(completion, "usage", None))
        _emit_token_log("LLM_MANUAL_PARSE", used_tokens, f"trace={trace_id or 'UNKNOWN'}")
        raw = completion.choices[0].message.content
        data = json.loads(raw) if isinstance(raw, str) else {}
        if not isinstance(data, dict):
            raise ValueError("invalid json content")
        return _normalize_manual_data({**base, **data})
    except Exception as e:
        _emit_token_log("LLM_MANUAL_PARSE", 0, f"trace={trace_id or 'UNKNOWN'};status=error;error={type(e).__name__}")
        logger.exception("Error parsing manual record with OpenAI: %s", e)
        return _parse_manual_record_fallback(base, user_text)


def _parse_manual_record_fallback(base: dict, user_text: str) -> dict:
    updated = dict(base)
    text = (user_text or "").strip()

    date_match = re.search(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", text)
    if date_match:
        y, m, d = date_match.groups()
        updated["date"] = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"

    if "空白收據" in text:
        updated["receipt_type"] = "空白收據"
    elif "發票" in text:
        updated["receipt_type"] = "發票"
    elif "收據" in text:
        updated["receipt_type"] = "收據"
    elif text in {"無", "沒有"} or "改無" in text:
        updated["receipt_type"] = "無"

    if "日常" in text:
        updated["category"] = "日常開銷"
    elif "設備" in text or "硬體" in text:
        updated["category"] = "設備購置"
    elif "社課" in text:
        updated["category"] = "社課開銷"
    elif "活動" in text:
        updated["category"] = "活動開銷"

    if re.fullmatch(r"\d+", text):
        updated["amount"] = int(text)
    elif any(k in text.lower() for k in ["金額", "總金額", "amount", "amt", "$", "元"]):
        amount_val = None
        m = re.search(r"(?:金額|總金額|amount|amt|\$|元)\D*(-?\d+)", text, flags=re.IGNORECASE)
        if m:
            amount_val = m.group(1)
        else:
            nums = re.findall(r"-?\d+", text)
            if nums:
                amount_val = nums[-1]
        if amount_val is not None:
            updated["amount"] = max(0, int(amount_val))

    item_patterns = [
        r"(?:消費)?品項(?:改成|改為|是|:|：)?\s*(.+)$",
        r"項目(?:改成|改為|是|:|：)?\s*(.+)$",
    ]
    for p in item_patterns:
        m = re.search(p, text)
        if m:
            updated["item_name"] = m.group(1).strip()
            break

    if not re.search(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", text) and not any(
        k in text for k in ["收據", "發票", "無", "日常", "設備", "社課", "活動", "金額", "總金額"]
    ):
        updated["item_name"] = text

    return _normalize_manual_data(updated)


def _infer_edit_targets(user_text: str) -> set[str]:
    text = (user_text or "").strip().lower()
    targets: set[str] = set()

    if re.fullmatch(r"\d+", text):
        targets.add("amount")
        return targets

    if any(k in text for k in ["金額", "總額", "amount", "total", "合計"]):
        targets.add("amount")
    if any(k in text for k in ["品項", "項目", "item", "items", "單價", "價格"]):
        targets.add("items")
    if any(k in text for k in ["日期", "date"]):
        targets.add("date")
    if any(k in text for k in ["賣方統編", "店家統編", "vendor"]) or ("賣方" in text and "統編" in text):
        targets.add("vendor_tax_id")
    if any(k in text for k in ["買方統編", "buyer"]) or ("買方" in text and "統編" in text):
        targets.add("buyer_tax_id")
    if "統編" in text and "賣方" not in text and "買方" not in text:
        targets.add("buyer_tax_id")
    if any(k in text for k in ["發票", "收據", "invoice_type", "類型"]):
        targets.add("invoice_type")
    if any(k in text for k in ["類別", "consumption_category"]):
        targets.add("consumption_category")
    return targets


def _sync_items_to_amount(items: list[dict], amount: int) -> list[dict]:
    if not items:
        return items
    if amount < 0:
        amount = 0

    if len(items) == 1:
        items[0]["price"] = amount
        return items

    current_sum = sum(max(0, int(it.get("price", 0) or 0)) for it in items)
    if current_sum <= 0:
        for i, it in enumerate(items):
            it["price"] = amount if i == len(items) - 1 else 0
        return items

    scaled = []
    running = 0
    for it in items[:-1]:
        p = max(0, int(it.get("price", 0) or 0))
        new_p = round(p * amount / current_sum)
        scaled.append(new_p)
        running += new_p
    scaled.append(max(0, amount - running))

    for i, it in enumerate(items):
        it["price"] = int(scaled[i])
    return items


def _normalize_after_edit(data: dict, user_text: str) -> dict:
    targets = _infer_edit_targets(user_text)
    normalized = dict(data)
    items = [dict(it) for it in normalized.get("items", [])]
    amount = int(normalized.get("amount", 0) or 0)

    if "amount" in targets and "items" not in targets:
        normalized["items"] = _sync_items_to_amount(items, amount)
    elif "items" in targets and "amount" not in targets:
        normalized["amount"] = sum(max(0, int(it.get("price", 0) or 0)) for it in items)
    elif re.fullmatch(r"\d+", (user_text or "").strip()):
        normalized["amount"] = int((user_text or "").strip())
        normalized["items"] = _sync_items_to_amount(items, normalized["amount"])

    return normalized


def _is_valid_tax_id(value: str) -> bool:
    return isinstance(value, str) and value.isdigit() and len(value) == 8


def _is_iso_date(value: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value or "")))


def _extract_quality_issues(data: InvoiceData) -> list[str]:
    issues = []
    if not _is_iso_date(data.date):
        issues.append("date_not_iso")
    if (data.amount or 0) <= 0:
        issues.append("amount_not_positive")
    if not data.items:
        issues.append("items_empty")
    if not _is_valid_tax_id(data.vendor_tax_id or ""):
        issues.append("vendor_tax_id_invalid")
    if not _is_valid_tax_id(data.buyer_tax_id or ""):
        issues.append("buyer_tax_id_invalid")

    target_buyer = (config.BUYER_TAX_ID or "").strip()
    if target_buyer and _is_valid_tax_id(data.buyer_tax_id or "") and data.buyer_tax_id != target_buyer:
        issues.append("buyer_tax_id_not_target")
    return issues


def _quality_score(data: InvoiceData) -> int:
    score = 0
    if _is_iso_date(data.date):
        score += 1
    if (data.amount or 0) > 0:
        score += 1
    if data.items:
        score += 1
    if _is_valid_tax_id(data.vendor_tax_id or ""):
        score += 1
    if _is_valid_tax_id(data.buyer_tax_id or ""):
        score += 1
    if (config.BUYER_TAX_ID or "").strip() and data.buyer_tax_id == (config.BUYER_TAX_ID or "").strip():
        score += 1
    return score


def _parse_invoice_once(base64_image: str, prompt: str, extra_context: str = "") -> tuple[InvoiceData, int]:
    content = [{"type": "text", "text": prompt}]
    if extra_context:
        content.append({"type": "text", "text": extra_context})
    content.append(
        {
            "type": "image_url",
            "image_url": {
                "url": f"data:image/jpeg;base64,{base64_image}",
            },
        }
    )

    completion = client.beta.chat.completions.parse(
        model=config.OPENAI_MODEL,
        messages=[{"role": "user", "content": content}],
        response_format=InvoiceData,
    )
    used_tokens = _extract_total_tokens(getattr(completion, "usage", None))
    return completion.choices[0].message.parsed, used_tokens


def extract_invoice_data(image_bytes: bytes, trace_id: str = "") -> InvoiceData:
    """
    Extract structured invoice fields from image bytes.
    """
    base64_image = base64.b64encode(image_bytes).decode("utf-8")

    target_buyer = (config.BUYER_TAX_ID or "29902605").strip()
    prompt = f"""
    Please extract invoice fields from the image and return structured data.
    Rules:
    1. date must be in YYYY-MM-DD.
    2. vendor_tax_id and buyer_tax_id should be 8-digit strings; use "" if missing.
    3. amount should be an integer total.
    4. items should list item name and price.
    4.1 invoice_type should be one of: 發票, 收據, 空白收據, 其他.
    5. Pay special attention to labels "買方" and "賣方". The target tax ID is usually right after these labels.
    6. For this system, buyer_tax_id is expected to be {target_buyer}. If this number appears near/after "買方", extract it as buyer_tax_id.
    7. Do not swap vendor and buyer IDs: vendor_tax_id should map to "賣方", buyer_tax_id should map to "買方".
    8. For total amount, prioritize fields around keywords: "總計", "合計", "應付", "總金額", "實收".
    9. If there are multiple 8-digit numbers, choose the nearest one after label "賣方" for vendor_tax_id and after label "買方" for buyer_tax_id.
    10. Return conservative values; do not hallucinate numbers not visible in the image.
    """
    total_tokens = 0

    try:
        first, first_tokens = _parse_invoice_once(base64_image, prompt)
        total_tokens += first_tokens
        first_issues = _extract_quality_issues(first)
        if not first_issues:
            _emit_token_log("LLM_INVOICE_EXTRACT", total_tokens, f"trace={trace_id or 'UNKNOWN'};passes=1")
            return first

        retry_hint = (
            "Retry with stricter extraction.\n"
            f"First-pass issues: {', '.join(first_issues)}\n"
            f"First-pass output: {first.model_dump()}\n"
            "Please re-read the image and correct likely mistakes.\n"
            "Focus on labels near tax IDs and amount keywords."
        )
        second, second_tokens = _parse_invoice_once(base64_image, prompt, retry_hint)
        total_tokens += second_tokens

        # Keep the better candidate after automatic validation.
        _emit_token_log("LLM_INVOICE_EXTRACT", total_tokens, f"trace={trace_id or 'UNKNOWN'};passes=2")
        if _quality_score(second) >= _quality_score(first):
            return second
        return first

    except Exception as e:
        _emit_token_log(
            "LLM_INVOICE_EXTRACT",
            total_tokens,
            f"trace={trace_id or 'UNKNOWN'};status=error;error={type(e).__name__}",
        )
        logger.exception("Error processing image with OpenAI: %s", e)
        return InvoiceData(
            date="1970-01-01",
            amount=0,
            items=[],
            invoice_type=_default_invoice_type(),
            consumption_category=_default_consumption_category(),
            vendor_tax_id="",
            buyer_tax_id="",
        )


def apply_user_edit(current_data: dict, user_text: str, trace_id: str = "") -> InvoiceData:
    """
    Update any editable invoice fields from free-form user text.
    Only fields explicitly mentioned in user_text should be changed.
    """
    clean_current = _sanitize_invoice_payload(current_data or {})
    if not clean_current:
        clean_current = InvoiceData(
            date="1970-01-01",
            amount=0,
            vendor_tax_id="",
            buyer_tax_id="",
            items=[],
            invoice_type=_default_invoice_type(),
            consumption_category=_default_consumption_category(),
        ).model_dump()

    prompt = (
        "You are editing parsed invoice data based on user correction text.\n"
        "Return the full updated object in the required schema.\n"
        "Rules:\n"
        "1) Change only fields explicitly requested by user.\n"
        "2) Keep all untouched fields exactly as current data.\n"
        "3) date must be YYYY-MM-DD.\n"
        "4) vendor_tax_id and buyer_tax_id must be 8-digit strings or empty string.\n"
        "5) amount must be an integer.\n"
        "6) If user only gives a single number (e.g., '160'), treat it as amount.\n"
    )

    try:
        completion = client.beta.chat.completions.parse(
            model=config.OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": prompt,
                },
                {
                    "role": "user",
                    "content": (
                        f"Current data:\n{clean_current}\n\n"
                        f"User correction:\n{user_text}"
                    ),
                },
            ],
            response_format=InvoiceData,
        )
        used_tokens = _extract_total_tokens(getattr(completion, "usage", None))
        _emit_token_log("LLM_APPLY_EDIT", used_tokens, f"trace={trace_id or 'UNKNOWN'}")
        parsed = completion.choices[0].message.parsed
        normalized = _normalize_after_edit(parsed.model_dump(), user_text)
        return InvoiceData(**normalized)
    except Exception as e:
        _emit_token_log("LLM_APPLY_EDIT", 0, f"trace={trace_id or 'UNKNOWN'};status=error;error={type(e).__name__}")
        logger.exception("Error applying user edit with OpenAI: %s", e)
        return _apply_user_edit_fallback(clean_current, user_text)


def _apply_user_edit_fallback(current_data: dict, user_text: str) -> InvoiceData:
    """
    Best-effort non-LLM fallback for common edits.
    """
    updated = dict(current_data)
    text = user_text.strip()

    if re.fullmatch(r"\d+", text):
        updated["amount"] = int(text)

    date_match = re.search(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", text)
    if date_match and any(k in text for k in ["日期", "date"]):
        y, m, d = date_match.groups()
        updated["date"] = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"

    amount_match = re.search(r"(金額|總額|amount)\D*(\d+)", text, flags=re.IGNORECASE)
    if amount_match:
        updated["amount"] = int(amount_match.group(2))

    if any(k in text for k in ["賣方統編", "店家統編", "vendor"]) or ("賣方" in text and "統編" in text):
        m = re.search(r"(\d{8})", text)
        if m:
            updated["vendor_tax_id"] = m.group(1)

    if any(k in text for k in ["買方統編", "buyer"]) or ("買方" in text and "統編" in text):
        m = re.search(r"(\d{8})", text)
        if m:
            updated["buyer_tax_id"] = m.group(1)
    elif "統編" in text and "賣方" not in text:
        m = re.search(r"(\d{8})", text)
        if m:
            updated["buyer_tax_id"] = m.group(1)

    if any(k in text for k in ["空白收據", "收據", "發票", "invoice_type", "類型"]):
        if "空白收據" in text:
            updated["invoice_type"] = "空白收據"
        elif "電子發票" in text or "發票" in text:
            updated["invoice_type"] = "發票"
        elif "收據" in text:
            updated["invoice_type"] = "收據"
        elif "其他" in text:
            updated["invoice_type"] = "其他"

    if "類別" in text or "consumption_category" in text:
        for cand in ["日常開銷與練習", "長期硬體設備購置", "社課開銷", "活動開銷", "未分類"]:
            if cand in text:
                updated["consumption_category"] = cand
                break

    if updated.get("invoice_type") not in InvoiceData.model_fields["invoice_type"].annotation.__args__:
        updated["invoice_type"] = _default_invoice_type()
    if (
        updated.get("consumption_category")
        not in InvoiceData.model_fields["consumption_category"].annotation.__args__
    ):
        updated["consumption_category"] = _default_consumption_category()

    updated = _normalize_after_edit(updated, user_text)
    return InvoiceData(**updated)
