import requests
from tenacity import nap

from pebble.wcl_client import WCLClient


def test_post_retries(monkeypatch):
    monkeypatch.setattr(nap, "sleep", lambda _: None)
    client = WCLClient("id", "secret")
    client._ensure_token = lambda: None  # bypass token retrieval

    attempts = {"count": 0}

    def fake_post(url, *args, **kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise requests.ConnectionError("boom")

        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {"data": {"ok": True}}

        return Resp()

    client._session.post = fake_post
    data = client._post("query")
    assert data == {"data": {"ok": True}}
    assert attempts["count"] == 3
