# SCUCT_AI LINE Bot

FastAPI + LINE Messaging API bot for:
- invoice image parsing (LLM)
- confirmation/edit/cancel card flow
- manual bookkeeping flow
- Google Sheets integration (Invoices, Subsidies, States, Log)

## Local Run
```powershell
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Run Tests
```powershell
pytest -q
```

## Deploy (Render)
This repo includes Render-ready files:
- render.yaml
- runtime.txt
- .env.example
- .gitignore

Set env vars in Render:
- LINE_CHANNEL_ACCESS_TOKEN
- LINE_CHANNEL_SECRET
- OPENAI_API_KEY
- OPENAI_MODEL (default: gpt-5-mini)
- GOOGLE_SHEET_ID
- GOOGLE_SERVICE_ACCOUNT_JSON
- BUYER_TAX_ID (default: 29902605)
- DAILY_STATE_RESET_ENABLED (default: true)
- DAILY_STATE_RESET_HOUR (default: 8, Taiwan time)
- DAILY_STATE_RESET_MINUTE (default: 0, Taiwan time)
- ADMIN_LINE_IDS (comma-separated LINE user IDs for finance/admin commands)
- DEBUG_ERROR_LOG_MAX_BYTES (default: 2000000)
- HOURLY_KEEPALIVE_ENABLED (default: true)
- HOURLY_KEEPALIVE_INTERVAL_MINUTES (default: 10)
- HOURLY_KEEPALIVE_URL (optional, fallback to PUBLIC_BASE_URL)

Optional:
- PUBLIC_BASE_URL
- LIFF_ID
- GEMINI_API_KEY

## LINE Webhook
After deploy, set webhook URL in LINE Developers:
`https://<your-render-domain>/webhook`

## Finance Admin Commands
- `執行配對`: match newly eligible invoices that are currently unmatched.
- `重新執行配對`: reset current matches and recalculate all eligible matches.
- `補助總覽`: show subsidy usage and gap overview.
- `補助品項查詢 <活動ID>`: list matched invoices/items for one activity.
- `核銷夠不夠用 <活動ID>`: show whether one activity has enough matched invoices.
- `財務使用教學`: show the finance operation guide.

## Periodic Background Task
- The app runs a periodic background task for keepalive.
- If `HOURLY_KEEPALIVE_URL` (or `PUBLIC_BASE_URL`) is set, it sends a GET ping every interval.
- Default interval is 10 minutes (`HOURLY_KEEPALIVE_INTERVAL_MINUTES=10`).
- Render free instances can still sleep due platform inactivity rules. To reduce sleep chance, use an external uptime ping service with interval < 15 minutes.

## Note
Do not commit secrets (.env, API keys, service-account JSON).
