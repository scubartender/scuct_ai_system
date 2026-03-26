from core.schemas import UserState
from services.sheets_service import LOG_HEADERS, STATES_HEADERS, SheetsService


class _FakeCell:
    def __init__(self, row: int):
        self.row = row


class _FakeStatesSheet:
    def __init__(self):
        self.rows = {
            1: list(STATES_HEADERS),
            2: ["u-1", "Alice", "WAITING_FOR_CONFIRM", '{"amount": 10}', "2026-03-19T00:00:00+08:00"],
        }
        self.find_calls = 0
        self.update_calls = []
        self.append_calls = []

    def find(self, query, in_column=1):
        self.find_calls += 1
        for idx, row in self.rows.items():
            if idx == 1:
                continue
            if row and row[0] == query:
                return _FakeCell(idx)
        return None

    def row_values(self, row_idx):
        return list(self.rows.get(row_idx, []))

    def update(self, rng, values):
        self.update_calls.append((rng, values))
        row_idx = int(rng.split(":")[0][1:])
        self.rows[row_idx] = list(values[0])

    def append_row(self, row):
        self.append_calls.append(list(row))
        new_idx = max(self.rows) + 1
        self.rows[new_idx] = list(row)


class _FakeLogSheet:
    def __init__(self, values):
        self._values = values
        self.updated = None

    def get_all_values(self):
        return [list(r) for r in self._values]

    def update(self, rng, values):
        self.updated = (rng, values)


def _build_service_for_states(fake_states_sheet: _FakeStatesSheet) -> SheetsService:
    svc = SheetsService.__new__(SheetsService)
    svc.states_sheet = fake_states_sheet
    svc._state_row_cache = {}
    return svc


def test_state_row_cache_reuses_find_results():
    fake_sheet = _FakeStatesSheet()
    svc = _build_service_for_states(fake_sheet)

    assert svc.user_exists("u-1") is True
    assert svc.user_exists("u-1") is True
    assert fake_sheet.find_calls == 1

    state = svc.get_user_state("u-1")
    assert state.user_name == "Alice"
    assert state.state == "WAITING_FOR_CONFIRM"
    # get_user_state should use cached row index and avoid a new find call.
    assert fake_sheet.find_calls == 1


def test_set_user_state_updates_existing_row_without_extra_find():
    fake_sheet = _FakeStatesSheet()
    svc = _build_service_for_states(fake_sheet)

    assert svc.user_exists("u-1") is True
    assert fake_sheet.find_calls == 1

    new_state = UserState(
        line_id="u-1",
        user_name="Alice",
        state="NORMAL",
        temp_data="",
        last_used="2026-03-19T01:00:00+08:00",
    )
    svc.set_user_state(new_state)

    assert fake_sheet.find_calls == 1
    assert fake_sheet.update_calls
    assert fake_sheet.update_calls[-1][0] == "A2:E2"


def test_migrate_log_sheet_v1_to_v2():
    old_rows = [
        ["Timestamp", "Action", "Details", "Token"],
        ["2026-03-19T00:00:00+08:00", "LLM_APPLY_EDIT", "trace=u-1:r-1", "42"],
    ]
    fake_log = _FakeLogSheet(old_rows)

    svc = SheetsService.__new__(SheetsService)
    svc.log_sheet = fake_log
    svc._migrate_log_sheet_v1_to_v2()

    assert fake_log.updated is not None
    rng, values = fake_log.updated
    assert rng == "A1:E2"
    assert values[0] == LOG_HEADERS
    assert values[1] == ["2026-03-19T00:00:00+08:00", "LLM_APPLY_EDIT", "", "trace=u-1:r-1", "42"]
