import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()


def _safe_int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default


def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_csv_env_set(name: str) -> set[str]:
    raw = os.environ.get(name, "")
    return {part.strip() for part in raw.split(",") if part and part.strip()}


class Config:
    # LINE Bot settings
    LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
    LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
    
    # LLM settings
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
    OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5-mini").strip() or "gpt-5-mini"
    
    # Google Sheets settings
    GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")

    # LIFF / public URL settings
    LIFF_ID = os.environ.get("LIFF_ID", "")
    PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "")
    
    # Business logic settings
    BUYER_TAX_ID = os.environ.get("BUYER_TAX_ID", "29902605")
    DAILY_STATE_RESET_ENABLED = _parse_bool_env("DAILY_STATE_RESET_ENABLED", True)
    DAILY_STATE_RESET_HOUR = _safe_int_env("DAILY_STATE_RESET_HOUR", 8)
    DAILY_STATE_RESET_MINUTE = _safe_int_env("DAILY_STATE_RESET_MINUTE", 0)
    ADMIN_LINE_IDS = _parse_csv_env_set("ADMIN_LINE_IDS")
    DEBUG_ERROR_LOG_MAX_BYTES = _safe_int_env("DEBUG_ERROR_LOG_MAX_BYTES", 2_000_000)
    HOURLY_KEEPALIVE_ENABLED = _parse_bool_env("HOURLY_KEEPALIVE_ENABLED", True)
    HOURLY_KEEPALIVE_INTERVAL_MINUTES = max(1, _safe_int_env("HOURLY_KEEPALIVE_INTERVAL_MINUTES", 10))
    HOURLY_KEEPALIVE_URL = os.environ.get("HOURLY_KEEPALIVE_URL", "").strip()

config = Config()
