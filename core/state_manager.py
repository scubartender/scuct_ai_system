import json
from enum import Enum
from typing import Optional

from core.schemas import UserState

# Enum for states to prevent typos
class AppState(str, Enum):
    NORMAL = "NORMAL"
    WAITING_FOR_INFO = "WAITING_FOR_INFO"
    WAITING_FOR_CONFIRM = "WAITING_FOR_CONFIRM"

class StateManager:
    def __init__(self, sheets_service):
        self.sheets = sheets_service
        
    def user_exists(self, line_id: str) -> bool:
        return self.sheets.user_exists(line_id)

    def get_state(self, line_id: str) -> UserState:
        """Fetch user state from Sheets"""
        return self.sheets.get_user_state(line_id)
        
    def _now_iso(self) -> str:
        return self.sheets.get_taiwan_time().isoformat()

    def set_state(
        self,
        line_id: str,
        state: AppState,
        temp_data: dict = None,
        user_name: str = None,
        current_state: Optional[UserState] = None,
    ):
        """Save user state to Sheets"""
        current = current_state or self.get_state(line_id)
        resolved_user_name = (user_name or current.user_name or "Unknown").strip() or "Unknown"
        user_state = UserState(
            line_id=line_id,
            user_name=resolved_user_name,
            state=state.value,
            temp_data=json.dumps(temp_data, ensure_ascii=False) if temp_data else None,
            last_used=self._now_iso(),
        )
        self.sheets.set_user_state(user_state)
        
    def clear_state(self, line_id: str, user_name: str = None, current_state: Optional[UserState] = None):
        """Reset user state to NORMAL"""
        self.set_state(line_id, AppState.NORMAL, None, user_name=user_name, current_state=current_state)

    def touch_user(self, line_id: str, user_name: str = None, current_state: Optional[UserState] = None):
        """Ensure user exists in States and refresh last-used timestamp."""
        current = current_state or self.get_state(line_id)
        resolved_user_name = (user_name or current.user_name or "Unknown").strip() or "Unknown"
        touched = UserState(
            line_id=line_id,
            user_name=resolved_user_name,
            state=current.state,
            temp_data=current.temp_data,
            last_used=self._now_iso(),
        )
        self.sheets.set_user_state(touched)
        
    def get_temp_data(self, line_id: str) -> dict:
        """Parse temp JSON data from state"""
        state = self.get_state(line_id)
        if state.temp_data:
            try:
                return json.loads(state.temp_data)
            except json.JSONDecodeError:
                return {}
        return {}
