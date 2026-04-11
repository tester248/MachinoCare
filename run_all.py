from __future__ import annotations

import signal
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def start_processes() -> tuple[subprocess.Popen, subprocess.Popen]:
    backend_cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "backend.main:app",
        "--host",
        "0.0.0.0",
        "--port",
        "8000",
        "--reload",
    ]
    dashboard_cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(ROOT / "dashboard" / "app.py"),
        "--server.port",
        "8501",
    ]

    backend = subprocess.Popen(backend_cmd, cwd=ROOT)
    dashboard = subprocess.Popen(dashboard_cmd, cwd=ROOT)
    return backend, dashboard


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()


if __name__ == "__main__":
    backend_proc, dashboard_proc = start_processes()

    def _handle_signal(signum: int, frame: object) -> None:
        del signum, frame
        stop_process(backend_proc)
        stop_process(dashboard_proc)
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    backend_code = backend_proc.wait()
    dashboard_code = dashboard_proc.wait()
    raise SystemExit(max(backend_code, dashboard_code))
