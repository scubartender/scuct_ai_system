from fastapi.testclient import TestClient

import main


def test_root_endpoint():
    client = TestClient(main.app)
    response = client.get("/")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"


def test_webhook_requires_signature():
    client = TestClient(main.app)
    response = client.post("/webhook", content="{}")
    assert response.status_code == 400
    assert response.json()["detail"] == "Missing X-Line-Signature header"
