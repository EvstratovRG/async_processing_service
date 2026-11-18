"""Проверка Docker Compose с настоящим HTTP-получателем webhook."""

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Queue
from typing import Any, Iterator
from uuid import uuid4

import httpx

callbacks: Queue[dict[str, Any]] = Queue()


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        callbacks.put(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
        self.send_response(204)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


@contextmanager
def webhook_receiver() -> Iterator[None]:
    server = ThreadingHTTPServer(("0.0.0.0", 9000), Receiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def check_api(client: httpx.Client, body: dict[str, Any]) -> set[str]:
    assert client.post("/api/v1/payments", json=body).status_code == 401
    client.headers.update({"X-API-Key": os.environ["API_KEY"], "Idempotency-Key": str(uuid4())})
    assert client.post("/api/v1/payments", json={**body, "amount": "-1"}).status_code == 422

    def create(_index: int) -> httpx.Response:
        return client.post("/api/v1/payments", json=body)

    with ThreadPoolExecutor(max_workers=5) as executor:
        responses = list(executor.map(create, range(5)))
    assert all(response.status_code == 202 for response in responses), [r.text for r in responses]
    ids = {response.json()["payment_id"] for response in responses}
    assert len(ids) == 1
    assert client.post("/api/v1/payments", json={**body, "amount": "1"}).status_code == 409
    return ids


def check_processing(client: httpx.Client, body: dict[str, Any], ids: set[str]) -> None:
    event = callbacks.get(timeout=30)
    assert event["payment_id"] in ids
    assert event["status"] in ("succeeded", "failed")
    detail = client.get(f"/api/v1/payments/{event['payment_id']}")
    assert detail.status_code == 200
    assert detail.json()["status"] == event["status"]
    assert detail.json()["metadata"] == body["metadata"]
    assert client.get(f"/api/v1/payments/{uuid4()}").status_code == 404


def main() -> None:
    body = {
        "amount": "123.45",
        "currency": "RUB",
        "description": "Smoke test",
        "metadata": {"check": True},
        "webhook_url": os.environ.get("WEBHOOK_BASE_URL", "http://api:9000") + "/webhook",
    }
    with (
        webhook_receiver(),
        httpx.Client(
            base_url=os.environ.get("API_BASE_URL", "http://localhost:8000"), timeout=10
        ) as client,
    ):
        ids = check_api(client, body)
        check_processing(client, body, ids)
    print(
        "Smoke passed: auth, validation, concurrent idempotency, conflict, processing, webhook, GET"
    )


if __name__ == "__main__":
    main()
