import base64
import hashlib
import hmac

from fastapi.testclient import TestClient

import main
from config import config


def _sign(body: str, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def test_webhook_rejects_invalid_signature(monkeypatch):
    monkeypatch.setattr(config, "LINE_CHANNEL_SECRET", "unit-test-secret")
    client = TestClient(main.app)
    response = client.post("/webhook", headers={"X-Line-Signature": "invalid"}, content="{}")
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid signature"


def test_webhook_accepts_valid_signature_and_runs_handler(monkeypatch):
    secret = "unit-test-secret"
    body = '{"events":[]}'
    signature = _sign(body, secret)
    calls = []

    monkeypatch.setattr(config, "LINE_CHANNEL_SECRET", secret)
    monkeypatch.setattr(main.handler, "handle", lambda b, s: calls.append((b, s)))

    client = TestClient(main.app)
    response = client.post("/webhook", headers={"X-Line-Signature": signature}, content=body)

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert calls == [(body, signature)]
