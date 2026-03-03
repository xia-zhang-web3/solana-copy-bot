#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HOST = os.environ.get("MOCK_UPSTREAM_HOST", "127.0.0.1")
PORT = int(os.environ.get("MOCK_UPSTREAM_PORT", "18080"))
STATE_DIR = Path(
    os.environ.get(
        "MOCK_UPSTREAM_STATE_DIR",
        "/var/www/solana-copy-bot/state/mock_execution_upstream",
    )
)
STATE_DIR.mkdir(parents=True, exist_ok=True)
REQUEST_LOG = STATE_DIR / "requests.jsonl"
HEALTH_PATH = "/healthz"
DEFAULT_SIGNATURE = os.environ.get(
    "MOCK_UPSTREAM_TX_SIGNATURE",
    "5sbb6yhpDC9uiGkGD72bFwWzPKenWpTEBsfBqw1wGin3VRG7j4vVij1iiT91Vg77SpBoBiAScJHmQ2dTDGJ1maUB",
)


def now_rfc3339() -> str:
    return (
        datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )


def append_request_log(record: dict) -> None:
    with REQUEST_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=True) + "\n")


class Handler(BaseHTTPRequestHandler):
    server_version = "copybot-mock-upstream/1.0"

    def _json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        return

    def do_GET(self) -> None:
        if self.path == HEALTH_PATH:
            self._json(
                200,
                {
                    "status": "ok",
                    "service": "mock_execution_upstream",
                    "version": "v1",
                    "ts": now_rfc3339(),
                },
            )
            return
        self._json(
            404,
            {
                "status": "reject",
                "retryable": False,
                "code": "not_found",
                "detail": "unknown path",
            },
        )

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            self._json(
                200,
                {
                    "status": "reject",
                    "retryable": False,
                    "code": "invalid_json",
                    "detail": f"mock invalid json: {exc}",
                },
            )
            return

        route = str(body.get("route", "rpc")).strip().lower() or "rpc"
        contract_version = str(body.get("contract_version", "v1")).strip() or "v1"
        request_id = str(body.get("request_id", "")).strip()
        client_order_id = str(body.get("client_order_id", "")).strip()

        append_request_log(
            {
                "ts": now_rfc3339(),
                "method": "POST",
                "path": self.path,
                "route": route,
                "contract_version": contract_version,
                "request_id": request_id,
                "client_order_id": client_order_id,
                "body": body,
            }
        )

        if self.path == "/simulate":
            self._json(
                200,
                {
                    "status": "ok",
                    "ok": True,
                    "accepted": True,
                    "route": route,
                    "contract_version": contract_version,
                    "request_id": request_id,
                    "detail": "mock_simulation_ok",
                },
            )
            return

        if self.path == "/submit":
            self._json(
                200,
                {
                    "status": "ok",
                    "ok": True,
                    "accepted": True,
                    "route": route,
                    "contract_version": contract_version,
                    "request_id": request_id,
                    "client_order_id": client_order_id,
                    "tx_signature": DEFAULT_SIGNATURE,
                    "submitted_at": now_rfc3339(),
                    "detail": "mock_submit_ok",
                },
            )
            return

        self._json(
            404,
            {
                "status": "reject",
                "retryable": False,
                "code": "not_found",
                "detail": "unknown path",
            },
        )


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"mock upstream listening on http://{HOST}:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
