from fastapi.testclient import TestClient

from apps.api.main import app


client = TestClient(app)


def test_health_endpoint() -> None:
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["openai_model"] == "gpt-5.4"


def test_workflow_stage_endpoint() -> None:
    response = client.get("/api/v1/workflow/stages")
    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["stage"] == "intake"
    assert payload[-1]["stage"] == "notebook_commit"
