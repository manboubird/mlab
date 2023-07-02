from fastapi.testclient import TestClient

from .main import app

client = TestClient(app)


def test_search():
    res = client.get("/search", headers={}, params={"q":"ralph"})
    assert res.status_code == 200
    assert res.json() == {
        "result": ""
    }
