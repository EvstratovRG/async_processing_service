"""Run against the compose API; starts a temporary webhook receiver on port 9000."""

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Queue
from typing import Any
from uuid import uuid4

import httpx

callbacks: Queue[dict[str, Any]] = Queue()


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        payload: dict[str, Any] = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        callbacks.put(payload)
        self.send_response(204)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


def main() -> None:
    server: ThreadingHTTPServer = ThreadingHTTPServer(("0.0.0.0", 9000), Receiver)
    thread: threading.Thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url: str = os.environ.get("API_BASE_URL", "http://localhost:8000")
    callback_url: str = os.environ.get("WEBHOOK_BASE_URL", "http://api:9000")
    key: str = str(uuid4())
    body: dict[str, Any] = {
        "amount": "123.45",
        "currency": "RUB",
        "description": "Smoke test",
        "metadata": {"check": True},
        "webhook_url": callback_url + "/webhook",
    }
    try:
        with httpx.Client(base_url=base_url, timeout=10) as client:
            assert client.post("/api/v1/payments", json=body).status_code == 401
            client.headers.update({"X-API-Key": os.environ["API_KEY"], "Idempotency-Key": key})
            assert client.post("/api/v1/payments", json={**body, "amount": "-1"}).status_code == 422

            def create(_index: int) -> httpx.Response:
                return client.post("/api/v1/payments", json=body)

            with ThreadPoolExecutor(max_workers=5) as executor:
                responses: list[httpx.Response] = list(executor.map(create, range(5)))
            assert all(response.status_code == 202 for response in responses), [
                response.text for response in responses
            ]
            ids: set[str] = {response.json()["payment_id"] for response in responses}
            assert len(ids) == 1
            assert client.post("/api/v1/payments", json={**body, "amount": "1"}).status_code == 409
            event: dict[str, Any] = callbacks.get(timeout=30)
            assert event["payment_id"] in ids
            assert event["status"] in ("succeeded", "failed")
            detail: httpx.Response = client.get(f"/api/v1/payments/{event['payment_id']}")
            assert detail.status_code == 200
            assert detail.json()["status"] == event["status"]
            assert detail.json()["metadata"] == body["metadata"]
            assert client.get(f"/api/v1/payments/{uuid4()}").status_code == 404
            print(
                "Smoke passed: auth, validation, concurrent idempotency, conflict, processing, webhook, GET"
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
