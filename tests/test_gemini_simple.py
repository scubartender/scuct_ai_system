from services.llm_service import _parse_manual_record_fallback


def test_parse_manual_record_fallback_amount_and_date():
    base = {
        "date": "2026-03-01",
        "receipt_type": "無",
        "item_name": "",
        "category": "日常開銷",
        "amount": 0,
    }
    result = _parse_manual_record_fallback(base, "2026-03-19 amount 250")
    assert result["date"] == "2026-03-19"
    assert result["amount"] == 250


def test_parse_manual_record_fallback_receipt_and_category():
    base = {
        "date": "2026-03-01",
        "receipt_type": "無",
        "item_name": "",
        "category": "日常開銷",
        "amount": 0,
    }
    result = _parse_manual_record_fallback(base, "\u793e\u8ab2\u6750\u6599 \u6536\u64da")
    assert result["receipt_type"] == "收據"
    assert result["category"] == "社課開銷"
