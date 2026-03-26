from core.schemas import InvoiceData, InvoiceItem
from main import _extract_bookkeep_payload, _looks_like_invoice_data


def test_extract_bookkeep_payload_with_inline_text():
    ok, payload = _extract_bookkeep_payload("記帳 今天買檸檬 200")
    assert ok is True
    assert payload == "今天買檸檬 200"


def test_extract_bookkeep_payload_without_trigger():
    ok, payload = _extract_bookkeep_payload("今天買檸檬 200")
    assert ok is False
    assert payload == ""


def test_looks_like_invoice_data_rejects_placeholder_only():
    data = InvoiceData(
        date="1970-01-01",
        amount=0,
        vendor_tax_id="",
        buyer_tax_id="",
        items=[],
        invoice_type="發票",
        consumption_category="未分類",
    )
    assert _looks_like_invoice_data(data) is False


def test_looks_like_invoice_data_accepts_real_signals():
    data = InvoiceData(
        date="2026-03-16",
        amount=120,
        vendor_tax_id="",
        buyer_tax_id="",
        items=[InvoiceItem(name="檸檬汁", price=120)],
        invoice_type="收據",
        consumption_category="日常開銷與練習",
    )
    assert _looks_like_invoice_data(data) is True


def test_looks_like_invoice_data_accepts_without_items():
    data = InvoiceData(
        date="2026-03-16",
        amount=120,
        vendor_tax_id="",
        buyer_tax_id="",
        items=[],
        invoice_type="發票",
        consumption_category="未分類",
    )
    assert _looks_like_invoice_data(data) is True
