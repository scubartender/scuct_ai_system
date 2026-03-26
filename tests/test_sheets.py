import re
from datetime import datetime

from services.sheets_service import INVOICES_HEADERS, STATES_HEADERS, SUBSIDIES_HEADERS, SheetsService


class _FakeStatesSheet:
    def __init__(self):
        self.values = [
            list(STATES_HEADERS),
            ["u1", "Alice", "WAITING_FOR_CONFIRM", '{"k":1}', "2026-03-19T08:00:00+08:00"],
            ["u2", "Bob", "NORMAL", "", "2026-03-19T08:00:00+08:00"],
        ]
        self.last_update = None

    def get_all_values(self):
        return [list(row) for row in self.values]

    def update(self, rng, rows):
        self.last_update = (rng, rows)
        self.values = [self.values[0], *[list(r) for r in rows]]


def test_reset_all_states_to_normal():
    svc = SheetsService.__new__(SheetsService)
    svc.states_sheet = _FakeStatesSheet()
    svc._state_row_cache = {"u1": 2}
    logs = []
    svc.log_action = lambda action, details, token=None, user="": logs.append((action, details, user))

    result = svc.reset_all_states_to_normal(reason="unit_test")

    assert result["total_rows"] == 2
    assert result["updated_rows"] == 1
    assert svc.states_sheet.last_update[0] == "A2:E3"
    assert svc.states_sheet.values[1][2] == "NORMAL"
    assert svc.states_sheet.values[2][2] == "NORMAL"
    assert svc._state_row_cache == {}
    assert logs and logs[0][0] == "RESET_ALL_STATES"


class _FakeInvoicesSheet:
    def __init__(self):
        self.values = [
            list(INVOICES_HEADERS),
            ["INV-1", "2026-03-19T00:00:00+08:00", "Alice", "2026-03-18", "發票", "500", "A", "活動開銷", "12345678", "1", "img", "0", ""],
            ["INV-2", "2026-03-19T00:00:00+08:00", "Bob", "2026-03-18", "發票", "300", "B", "活動開銷", "12345678", "1", "img", "1", "ACT-OLD"],
            ["INV-3", "2026-03-19T00:00:00+08:00", "Bob", "2026-03-18", "發票", "200", "C", "活動開銷", "12345678", "0", "img", "0", ""],
            ["INV-4", "2026-03-19T00:00:00+08:00", "Cathy", "2026-03-18", "發票", "100", "D", "活動開銷", "12345678", "2", "img", "0", ""],
        ]
        self.update_calls = []

    def get_all_values(self):
        return [list(r) for r in self.values]

    def update(self, rng, rows):
        self.update_calls.append((rng, rows))
        m = re.match(r"^([A-Z]+)(\d+):([A-Z]+)(\d+)$", rng)
        if not m:
            return
        c1, r1, c2, r2 = m.group(1), int(m.group(2)), m.group(3), int(m.group(4))
        assert c1 == "L" and c2 == "M"
        for idx, row_idx in enumerate(range(r1, r2 + 1)):
            while len(self.values[row_idx - 1]) < 13:
                self.values[row_idx - 1].append("")
            self.values[row_idx - 1][11] = str(rows[idx][0])
            self.values[row_idx - 1][12] = str(rows[idx][1])


class _FakeSubsidiesSheet:
    def __init__(self):
        self.values = [
            list(SUBSIDIES_HEADERS),
            ["ACT-1", "2026-03-01", "活動A", "1000", "100", "900", "", "", "", ""],
            ["ACT-2", "2026-03-02", "活動B", "500", "0", "500", "", "", "", ""],
        ]

    def get_all_values(self):
        return [list(r) for r in self.values]

    def update(self, rng, rows):
        m = re.match(r"^E(\d+):F(\d+)$", rng)
        if not m:
            return
        r1, r2 = int(m.group(1)), int(m.group(2))
        for idx, row_idx in enumerate(range(r1, r2 + 1)):
            while len(self.values[row_idx - 1]) < 6:
                self.values[row_idx - 1].append("")
            self.values[row_idx - 1][4] = str(rows[idx][0])
            self.values[row_idx - 1][5] = str(rows[idx][1])


class _FakeAppendInvoicesSheet:
    def __init__(self):
        self.rows = [list(INVOICES_HEADERS)]

    def col_values(self, idx):
        return [row[0] for row in self.rows]

    def append_row(self, row):
        self.rows.append(list(row))


def test_run_invoice_matching_non_rematch():
    svc = SheetsService.__new__(SheetsService)
    svc.invoices_sheet = _FakeInvoicesSheet()
    svc.subsidies_sheet = _FakeSubsidiesSheet()
    svc.log_action = lambda *args, **kwargs: None

    svc._update_subsidy_amounts = lambda **kwargs: None
    svc._greedy_match = lambda invoice_date, invoice_amount: (
        {
            "row_idx": 2,
            "activity_id": "ACT-1",
            "subsidy_amount": 1000.0,
            "current_accumulated": 100.0,
        }
        if invoice_amount == 500
        else None
    )

    result = svc.run_invoice_matching(rematch=False, user="UADMIN")
    assert result["processed"] == 2
    assert result["matched"] == 1
    assert result["unmatched"] == 1
    assert result["skipped"] == 2
    assert svc.invoices_sheet.values[1][11] == "1"
    assert svc.invoices_sheet.values[1][12] == "ACT-1"
    assert svc.invoices_sheet.values[2][11] == "1"
    assert svc.invoices_sheet.values[2][12] == "ACT-OLD"
    assert svc.invoices_sheet.values[4][11] == "0"


def test_run_invoice_matching_rematch_clears_and_rebuilds():
    svc = SheetsService.__new__(SheetsService)
    svc.invoices_sheet = _FakeInvoicesSheet()
    svc.subsidies_sheet = _FakeSubsidiesSheet()
    svc.log_action = lambda *args, **kwargs: None
    svc._update_subsidy_amounts = lambda **kwargs: None
    svc._greedy_match = lambda invoice_date, invoice_amount: None

    result = svc.run_invoice_matching(rematch=True, user="UADMIN")
    assert result["rematch"] is True
    assert result["subsidy_reset_rows"] == 2
    assert result["cleared_invoice_rows"] == 4
    assert svc.invoices_sheet.values[1][11] == "0"
    assert svc.invoices_sheet.values[2][11] == "0"
    assert svc.invoices_sheet.values[1][12] == ""
    assert svc.invoices_sheet.values[2][12] == ""


def test_save_invoice_without_auto_match():
    from core.schemas import InvoiceData, InvoiceItem

    svc = SheetsService.__new__(SheetsService)
    svc.invoices_sheet = _FakeAppendInvoicesSheet()
    svc.log_action = lambda *args, **kwargs: None
    svc.calculate_eligibility = lambda data: 1
    svc.get_taiwan_time = lambda: datetime(2026, 3, 19, 8, 0, 0)
    svc._greedy_match = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("_greedy_match should not be called"))

    data = InvoiceData(
        date="2026-03-19",
        amount=1000,
        vendor_tax_id="12345678",
        buyer_tax_id="29902605",
        items=[InvoiceItem(name="測試品項", price=1000)],
        invoice_type="發票",
        consumption_category="活動開銷",
    )
    result = svc.save_invoice_and_match("U1", "Admin", data, "LINE_MESSAGE_ID:1", auto_match=False)
    assert result["matched_activity"] == ""
