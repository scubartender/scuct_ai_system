import base64
import hashlib
import hmac

import main
from config import config


def test_line_signature_helper():
    secret = "unit-secret"
    body = '{"events":[]}'
    digest = hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).digest()
    signature = base64.b64encode(digest).decode("utf-8")

    original = config.LINE_CHANNEL_SECRET
    try:
        config.LINE_CHANNEL_SECRET = secret
        assert main._is_valid_line_signature(body, signature) is True
        assert main._is_valid_line_signature(body, "bad-signature") is False
    finally:
        config.LINE_CHANNEL_SECRET = original


def test_seconds_until_next_reset_is_positive():
    seconds = main._seconds_until_next_reset(8, 0)
    assert seconds > 0
