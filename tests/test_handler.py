from datetime import datetime, timedelta, timezone

from core.schemas import UserState
from core.state_manager import AppState, StateManager
from main import _extract_bookkeep_payload


class _FakeSheets:
    def __init__(self):
        self.state = UserState(
            line_id="u-1",
            user_name="Unknown",
            state="WAITING_FOR_INFO",
            temp_data="{}",
            last_used=None,
        )

    def user_exists(self, line_id: str) -> bool:
        return line_id == "u-1"

    def get_user_state(self, line_id: str) -> UserState:
        return self.state

    def set_user_state(self, state: UserState):
        self.state = state

    def get_taiwan_time(self):
        return datetime.now(timezone(timedelta(hours=8)))


def test_extract_bookkeep_payload():
    ok, payload = _extract_bookkeep_payload("\u8a18\u5e33: \u6e2c\u8a66 200")
    assert ok is True
    assert payload == "\u6e2c\u8a66 200"


def test_clear_state_keeps_explicit_display_name():
    sheets = _FakeSheets()
    manager = StateManager(sheets)
    current = manager.get_state("u-1")

    manager.touch_user("u-1", "Alice", current_state=current)
    manager.clear_state("u-1", user_name="Alice", current_state=current)

    assert sheets.state.state == AppState.NORMAL.value
    assert sheets.state.user_name == "Alice"
