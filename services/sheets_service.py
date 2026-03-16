import json
import os
import re
from datetime import datetime, timezone, timedelta
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials

from config import config
from core.schemas import InvoiceData, UserState

SERVICE_ACCOUNT_JSON_STR = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

INVOICES_HEADERS = [
    "流水號 (ID)",
    "上傳日期",
    "上傳者",
    "發票日期",
    "發票分類",
    "總金額",
    "消費品項",
    "消費分類",
    "公司統編",
    "核銷可用性",
    "發票照片網址",
    "核銷狀態",
    "核銷活動 ID",
]

SUBSIDIES_HEADERS = [
    "活動ID",
    "活動日期",
    "活動名稱",
    "補助金額",
    "目前累計發票",
    "核銷缺口",
    "截止日期",
    "預警狀態",
    "起始計算日",
    "核銷明細",
]

LOG_HEADERS = ["Timestamp", "Action", "Details", "Token"]
STATES_HEADERS = ["LINE ID", "User Name", "Current State", "Temp JSON", "Last used"]
STATES_HEADERS_V1 = ["LINE ID", "Current State", "Temp JSON"]


def get_gspread_client():
    if SERVICE_ACCOUNT_JSON_STR:
        creds_dict = json.loads(SERVICE_ACCOUNT_JSON_STR)
        credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(credentials)

    import google.auth

    credentials, _ = google.auth.default(scopes=SCOPES)
    return gspread.authorize(credentials)


class SheetsService:
    def __init__(self):
        self.client = get_gspread_client()
        self.doc = self.client.open_by_key(config.GOOGLE_SHEET_ID)

        self.invoices_sheet = self._get_or_create_sheet(["Invoices"], "Invoices")
        self.subsidies_sheet = self._get_or_create_sheet(["Subsidies", "Subsides", "subsides"], "Subsidies")
        self.states_sheet = self._get_or_create_sheet(["States"], "States")
        self.log_sheet = self._get_or_create_sheet(["Log"], "Log")

        self._init_headers()

    def _normalize_sheet_title(self, title: str) -> str:
        return str(title).replace(" ", "").replace("_", "").replace("-", "").strip().lower()

    def _find_sheet_by_aliases(self, aliases: list[str]):
        normalized_aliases = {self._normalize_sheet_title(a) for a in aliases}
        for ws in self.doc.worksheets():
            if self._normalize_sheet_title(ws.title) in normalized_aliases:
                return ws
        return None

    def _get_or_create_sheet(self, aliases: list[str], create_title: str):
        ws = self._find_sheet_by_aliases(aliases)
        if ws:
            return ws
        return self.doc.add_worksheet(title=create_title, rows=1000, cols=30)

    def _init_headers(self):
        invoice_header_row = self.invoices_sheet.row_values(1)
        if not invoice_header_row:
            self.invoices_sheet.append_row(INVOICES_HEADERS)
        elif invoice_header_row[: len(INVOICES_HEADERS)] != INVOICES_HEADERS:
            self.invoices_sheet.update("A1:M1", [INVOICES_HEADERS])

        if not self.subsidies_sheet.row_values(1):
            self.subsidies_sheet.append_row(SUBSIDIES_HEADERS)

        states_header_row = self.states_sheet.row_values(1)
        if not states_header_row:
            self.states_sheet.append_row(STATES_HEADERS)
        elif states_header_row[: len(STATES_HEADERS)] != STATES_HEADERS:
            if states_header_row[: len(STATES_HEADERS_V1)] == STATES_HEADERS_V1:
                self._migrate_states_sheet_v1_to_v2()
            else:
                self.states_sheet.update("A1:E1", [STATES_HEADERS])

        log_header_row = self.log_sheet.row_values(1)
        if not log_header_row:
            self.log_sheet.append_row(LOG_HEADERS)
        elif log_header_row[: len(LOG_HEADERS)] != LOG_HEADERS:
            self.log_sheet.update("A1:D1", [LOG_HEADERS])

    def _migrate_states_sheet_v1_to_v2(self):
        values = self.states_sheet.get_all_values()
        if not values:
            self.states_sheet.update("A1:E1", [STATES_HEADERS])
            return

        migrated_rows = [STATES_HEADERS]
        for row in values[1:]:
            line_id = row[0] if len(row) > 0 else ""
            state = row[1] if len(row) > 1 else "NORMAL"
            temp_json = row[2] if len(row) > 2 else ""
            migrated_rows.append([line_id, "Unknown", state or "NORMAL", temp_json, ""])

        end_row = len(migrated_rows)
        self.states_sheet.update(f"A1:E{end_row}", migrated_rows)

    def get_taiwan_time(self):
        tw_timezone = timezone(timedelta(hours=8))
        return datetime.now(tw_timezone)

    def log_action(self, action: str, details: str, token: Optional[int] = None):
        token_value = ""
        if token is not None:
            try:
                token_value = int(token)
            except Exception:
                token_value = str(token)
        self.log_sheet.append_row([self.get_taiwan_time().isoformat(), action, details, token_value])

    def log_token_usage(self, action: str, token: int, details: str = ""):
        safe_token = 0
        try:
            safe_token = max(0, int(token))
        except Exception:
            safe_token = 0
        self.log_action(action=action, details=details, token=safe_token)

    # --- State Management ---
    def user_exists(self, line_id: str) -> bool:
        return bool(self.states_sheet.find(line_id, in_column=1))

    def get_user_state(self, line_id: str) -> UserState:
        cell = self.states_sheet.find(line_id, in_column=1)
        if not cell:
            return UserState(line_id=line_id)

        row_values = self.states_sheet.row_values(cell.row)
        while len(row_values) < 5:
            row_values.append("")
        header = self.states_sheet.row_values(1)
        if header[: len(STATES_HEADERS)] == STATES_HEADERS:
            return UserState(
                line_id=row_values[0],
                user_name=row_values[1] or "Unknown",
                state=row_values[2] or "NORMAL",
                temp_data=row_values[3] if row_values[3] else None,
                last_used=row_values[4] if row_values[4] else None,
            )

        # Backward-compatible parsing for old 3-column layout.
        return UserState(
            line_id=row_values[0],
            user_name="Unknown",
            state=row_values[1] or "NORMAL",
            temp_data=row_values[2] if row_values[2] else None,
            last_used=None,
        )

    def set_user_state(self, state: UserState):
        cell = self.states_sheet.find(state.line_id, in_column=1)
        if cell:
            row_idx = cell.row
            self.states_sheet.update(f"A{row_idx}:E{row_idx}", [state.to_row()])
        else:
            self.states_sheet.append_row(state.to_row())

    # --- Eligibility Logic ---
    def _is_valid_tax_id(self, value: str) -> bool:
        return isinstance(value, str) and value.isdigit() and len(value) == 8

    def _is_blank_receipt_type(self, invoice_type: str) -> bool:
        t = str(invoice_type or "").strip()
        return t in {"空白收據", "空白 收據", "空白", "白單"}

    def _is_data_complete(self, data: InvoiceData, require_tax_ids: bool = True) -> bool:
        if not data.date or data.date == "1970-01-01":
            return False
        if (data.amount or 0) <= 0:
            return False
        if not data.items:
            return False

        for item in data.items:
            if not (item.name or "").strip():
                return False

        if require_tax_ids:
            if not self._is_valid_tax_id(data.vendor_tax_id or ""):
                return False
            if not self._is_valid_tax_id(data.buyer_tax_id or ""):
                return False

        return True

    def calculate_eligibility(self, data: InvoiceData) -> int:
        """
        0: 不可核銷
        1: 可核銷且金額 >= 500
        2: 可核銷且金額 < 500

        Rules:
        - 一般收據/發票：買方統編必須為 config.BUYER_TAX_ID 才可核銷
        - 空白收據：可免統編核銷
        """
        invoice_type = str(data.invoice_type or "").strip()
        is_blank_receipt = self._is_blank_receipt_type(invoice_type)

        if not self._is_data_complete(data, require_tax_ids=not is_blank_receipt):
            return 0

        if not is_blank_receipt:
            target_buyer = (config.BUYER_TAX_ID or "").strip()
            if target_buyer and (data.buyer_tax_id or "").strip() != target_buyer:
                return 0

        if data.amount >= 500:
            return 1
        return 2

    # --- Core Logic ---
    def save_invoice_and_match(self, user_id: str, display_name: str, data: InvoiceData, image_url: str) -> dict:
        eligibility = self.calculate_eligibility(data)

        now = self.get_taiwan_time()
        next_id_num = len(self.invoices_sheet.col_values(1))
        inv_id = f"INV-{now.strftime('%Y%m%d')}-{str(next_id_num).zfill(3)}"

        items_str = ", ".join([f"{item.name}" for item in data.items])

        matched_activity_id = ""
        reconciliation_status = 0

        if eligibility in (1, 2):
            matched_activity = self._greedy_match(data.date, int(data.amount))
            if matched_activity:
                matched_activity_id = matched_activity["activity_id"]
                reconciliation_status = 1
                self._update_subsidy_amounts(
                    row_idx=matched_activity["row_idx"],
                    subsidy_amount=matched_activity["subsidy_amount"],
                    current_accumulated=matched_activity["current_accumulated"],
                    invoice_amount=data.amount,
                )

        company_tax_id = (data.vendor_tax_id or "").strip() or "都沒有"

        row = [
            inv_id,
            now.isoformat(),
            display_name or user_id,
            data.date,
            data.invoice_type,
            int(data.amount),
            items_str,
            data.consumption_category,
            company_tax_id,
            eligibility,
            image_url,
            reconciliation_status,
            matched_activity_id,
        ]

        self.invoices_sheet.append_row(row)
        self.log_action(
            "SAVE_INVOICE",
            f"Saved {inv_id}. eligibility={eligibility}, matched_activity={matched_activity_id or 'NONE'}",
        )

        return {
            "invoice_id": inv_id,
            "eligibility": eligibility,
            "matched_activity": matched_activity_id,
        }

    def save_manual_record(
        self,
        user_id: str,
        display_name: str,
        record_date: str,
        receipt_type: str,
        item_name: str,
        category: str,
        amount: int,
    ) -> dict:
        now = self.get_taiwan_time()
        next_id_num = len(self.invoices_sheet.col_values(1))
        inv_id = f"MAN-{now.strftime('%Y%m%d')}-{str(next_id_num).zfill(3)}"

        safe_receipt_type = receipt_type if receipt_type in {"空白收據", "收據", "發票", "無"} else "無"
        safe_category = category if category in {"日常開銷", "設備購置", "社課開銷", "活動開銷"} else "日常開銷"
        safe_items = (item_name or "").strip() or "未填寫"
        safe_amount = max(0, int(amount))

        if safe_receipt_type == "空白收據":
            eligibility = 1 if safe_amount >= 500 else 2
        else:
            eligibility = 0

        row = [
            inv_id,  # 流水號 (ID)
            now.isoformat(),  # 上傳日期
            display_name or user_id,  # 上傳者
            record_date,  # 發票日期
            safe_receipt_type,  # 發票分類
            safe_amount,  # 總金額
            safe_items,  # 消費品項
            safe_category,  # 消費分類
            "都沒有",  # 公司統編
            eligibility,  # 核銷可用性
            "都沒有",  # 發票照片網址
            0,  # 核銷狀態
            "",  # 核銷活動 ID
        ]

        self.invoices_sheet.append_row(row)
        self.log_action("SAVE_MANUAL_RECORD", f"Saved {inv_id} by {display_name or user_id}")

        return {
            "invoice_id": inv_id,
            "eligibility": eligibility,
        }

    def _parse_date(self, value: str) -> Optional[datetime]:
        if value is None:
            return None

        if isinstance(value, datetime):
            return datetime(value.year, value.month, value.day)

        text = str(value).strip()
        if not text:
            return None

        normalized = (
            text.replace("年", "-")
            .replace("月", "-")
            .replace("日", "")
            .replace(".", "-")
            .replace("/", "-")
        )
        normalized = re.sub(r"\s+", " ", normalized)

        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(normalized, fmt)
                return datetime(dt.year, dt.month, dt.day)
            except Exception:
                pass

        try:
            iso_text = text.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso_text)
            return datetime(dt.year, dt.month, dt.day)
        except Exception:
            pass

        m = re.search(r"(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})", text)
        if not m:
            return None

        y, mo, d = m.groups()
        try:
            return datetime(int(y), int(mo), int(d))
        except Exception:
            return None

    def _to_float(self, value, default: float = 0.0) -> float:
        try:
            text = str(value).replace(",", "").strip()
            if text == "":
                return default
            return float(text)
        except Exception:
            return default

    def _calc_gap(self, subsidy_amount: float, current_accumulated: float) -> float:
        return round(max(0.0, subsidy_amount - current_accumulated), 2)

    def _normalize_key(self, key: str) -> str:
        return str(key).replace(" ", "").replace("\u3000", "").strip().lower()

    def _row_get(self, row: dict, *aliases: str):
        if not isinstance(row, dict):
            return ""
        normalized_map = {self._normalize_key(k): v for k, v in row.items()}
        for alias in aliases:
            v = normalized_map.get(self._normalize_key(alias))
            if v is not None and str(v).strip() != "":
                return v
        return ""

    def _normalize_activity_id(self, value) -> str:
        if value is None:
            return ""

        if isinstance(value, (int, float)):
            v = float(value)
            if v.is_integer():
                return str(int(v))
            return str(v)

        text = str(value).strip()
        if not text:
            return ""

        m = re.fullmatch(r"(\d+)\.0+", text)
        if m:
            return m.group(1)

        return text

    def _greedy_match(self, invoice_date_str: str, invoice_amount: int) -> Optional[dict]:
        invoice_date = self._parse_date(invoice_date_str)
        if not invoice_date:
            return None

        invoice_amount = max(0, int(invoice_amount or 0))
        if invoice_amount <= 0:
            return None

        records = self.subsidies_sheet.get_all_records()
        candidates = []

        for idx, row in enumerate(records):
            row_idx = idx + 2
            activity_id = self._normalize_activity_id(self._row_get(row, "活動ID", "活動 Id", "活動id"))
            activity_date_str = str(self._row_get(row, "活動日期", "活動 日期")).strip()
            activity_date = self._parse_date(activity_date_str)
            start_date = self._parse_date(str(self._row_get(row, "起始計算日", "起始 計算日")).strip())
            end_date = self._parse_date(str(self._row_get(row, "截止日期", "截止 日期")).strip())
            subsidy_amount = self._to_float(self._row_get(row, "補助金額", "補助 金額") or 0)
            current_accumulated = self._to_float(self._row_get(row, "目前累計發票", "目前 累計發票") or 0)
            # System-calculated gap: do not rely on manually entered "核銷缺口".
            gap = self._calc_gap(subsidy_amount, current_accumulated)

            if not activity_id or not activity_date:
                continue
            if subsidy_amount <= 0:
                continue
            if gap <= 0:
                continue
            # Only match activities that still have enough gap for this invoice amount.
            if gap < float(invoice_amount):
                continue
            # Prefer matching by configured accounting window when present.
            if start_date and invoice_date < start_date:
                continue
            if end_date and invoice_date > end_date:
                continue

            candidates.append(
                {
                    "row_idx": row_idx,
                    "activity_id": activity_id,
                    "activity_date": activity_date,
                    "subsidy_amount": subsidy_amount,
                    "current_accumulated": current_accumulated,
                    "gap": gap,
                }
            )

        if not candidates:
            return None

        # Greedy: prioritize earliest activity date, then smallest sufficient gap.
        candidates.sort(key=lambda x: (x["activity_date"], x["gap"]))
        return candidates[0]

    def _update_subsidy_amounts(
        self,
        row_idx: int,
        subsidy_amount: float,
        current_accumulated: float,
        invoice_amount: int,
    ):
        new_accumulated = round(current_accumulated + float(invoice_amount), 2)
        new_gap = self._calc_gap(subsidy_amount, new_accumulated)

        # Column E: 目前累計發票, Column F: 核銷缺口
        self.subsidies_sheet.update(f"E{row_idx}:F{row_idx}", [[new_accumulated, new_gap]])
