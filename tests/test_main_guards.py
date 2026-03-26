from linebot.v3.exceptions import InvalidSignatureError

import main
from config import config


def test_activation_and_usage_tokens():
    assert main._is_activation_token("\u597d") is True
    assert main._is_activation_token(" \u597d ") is True
    assert main._is_activation_token("OK") is False

    assert main._is_usage_guide_token("\u5982\u4f55\u4f7f\u7528") is True
    assert main._is_usage_guide_token("\u5982\u4f55 \u4f7f\u7528") is False


def test_should_confirm_amount_change_rules():
    assert main._should_confirm_amount_change("1600", old_amount=800, new_amount=1600) is True
    assert main._should_confirm_amount_change("\u91d1\u984d\u6539\u62101600", old_amount=800, new_amount=1600) is False
    assert main._should_confirm_amount_change("\u5e6b\u6211\u6539", old_amount=200, new_amount=60000) is True
    assert main._should_confirm_amount_change("1600", old_amount=1600, new_amount=1600) is False


def test_pending_amount_payload_and_meta_strip():
    base = {
        "_mode": "manual_bookkeeping",
        "amount": 100,
        "_image_message_id": "img-1",
        "_pending_amount_confirm": True,
    }
    candidate = {
        "_mode": "manual_bookkeeping",
        "amount": 150,
        "_pending_old_amount": 100,
    }

    payload = main._make_pending_amount_payload(base, candidate, old_amount=100, new_amount=150)
    assert payload["_pending_amount_confirm"] is True
    assert payload["_pending_old_amount"] == 100
    assert payload["_pending_new_amount"] == 150
    assert main._is_amount_confirm_pending(payload) is True
    assert "_pending_amount_confirm" not in payload["_pending_base_data"]
    assert "_pending_old_amount" not in payload["_pending_candidate_data"]

    stripped = main._strip_pending_amount_meta(payload)
    assert stripped["_mode"] == "manual_bookkeeping"
    assert stripped["_image_message_id"] == "img-1"
    assert "_pending_amount_confirm" not in stripped


def test_safe_handle_webhook_catches_invalid_signature(monkeypatch):
    calls = []

    class DummyHandler:
        def handle(self, body: str, signature: str):
            raise InvalidSignatureError("bad signature")

    def fake_log(context, exc, event=None):
        calls.append((context, type(exc).__name__))

    monkeypatch.setattr(main, "handler", DummyHandler())
    monkeypatch.setattr(main, "_log_runtime_exception", fake_log)
    main._safe_handle_webhook("{}", "invalid")

    assert calls
    assert calls[0][0] == "webhook_background_invalid_signature"


def test_finance_guide_keyword_detection():
    assert main._is_finance_guide_request("財務 使用教學") is True
    assert main._is_finance_guide_request("財務手冊") is True
    assert main._is_finance_guide_request("一般查詢") is False


def test_parse_admin_command():
    assert main._parse_admin_command("執行配對")[0] == "run_match"
    assert main._parse_admin_command("重新執行配對")[0] == "rematch"
    cmd, arg = main._parse_admin_command("補助品項查詢 114001")
    assert cmd == "activity_items"
    assert arg == "114001"
    cmd, arg = main._parse_admin_command("核銷夠不夠用 114001")
    assert cmd == "gap"
    assert arg == "114001"


def test_is_admin_user(monkeypatch):
    original = config.ADMIN_LINE_IDS
    try:
        config.ADMIN_LINE_IDS = {"UADMIN1"}
        assert main._is_admin_user("UADMIN1") is True
        assert main._is_admin_user("UOTHER") is False
    finally:
        config.ADMIN_LINE_IDS = original


def test_finance_guide_text_contains_commands():
    admin_text = main._finance_guide_text(is_admin=True)
    assert "執行配對" in admin_text
    user_text = main._finance_guide_text(is_admin=False)
    assert "不是財務/admin" in user_text


def test_handle_admin_command_run_match():
    class _FakeSheets:
        def run_invoice_matching(self, rematch: bool, user: str):
            assert rematch is False
            return {"rematch": False, "processed": 3, "matched": 2, "unmatched": 1, "skipped": 0, "unmatched_preview": []}

    class _FakeLine:
        def __init__(self):
            self.messages = []

        def reply_text(self, reply_token: str, text: str):
            self.messages.append((reply_token, text))

    fake_sheets = _FakeSheets()
    fake_line = _FakeLine()
    handled = main._handle_admin_command(
        user_id="UADMIN",
        display_name="Admin",
        reply_token="r1",
        text="執行配對",
        sheets_service=fake_sheets,
        line_service=fake_line,
    )
    assert handled is True
    assert fake_line.messages
    assert "執行配對完成" in fake_line.messages[0][1]
