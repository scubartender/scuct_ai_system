import pytest


def test_validate_runtime_settings_is_noop_when_disabled():
    from config import Config

    cfg = Config()
    cfg.REQUIRE_RUNTIME_CONFIG = False
    cfg.REQUIRED_RUNTIME_SETTINGS = ("LINE_CHANNEL_SECRET",)
    cfg.LINE_CHANNEL_SECRET = ""

    cfg.validate_runtime_settings()


def test_validate_runtime_settings_reports_missing_values():
    from config import Config

    cfg = Config()
    cfg.REQUIRE_RUNTIME_CONFIG = True
    cfg.REQUIRED_RUNTIME_SETTINGS = ("LINE_CHANNEL_SECRET", "OPENAI_API_KEY")
    cfg.LINE_CHANNEL_SECRET = ""
    cfg.OPENAI_API_KEY = "sk-test"

    with pytest.raises(RuntimeError) as exc_info:
        cfg.validate_runtime_settings()

    assert "LINE_CHANNEL_SECRET" in str(exc_info.value)
    assert "OPENAI_API_KEY" not in str(exc_info.value)
