from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def start_processes() -> tuple[subprocess.Popen, subprocess.Popen]:
    backend_host = os.getenv("BACKEND_HOST", "0.0.0.0")
    backend_port = os.getenv("BACKEND_PORT", "8000")
    dashboard_host = os.getenv("DASHBOARD_HOST", "0.0.0.0")
    dashboard_port = os.getenv("DASHBOARD_PORT", "8501")
    enable_reload = os.getenv("ENABLE_RELOAD", "0") == "1"

    backend_cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "backend.main:app",
        "--host",
        backend_host,
        "--port",
        backend_port,
    ]
    if enable_reload:
        backend_cmd.append("--reload")

    dashboard_cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(ROOT / "dashboard" / "app.py"),
        "--server.address",
        dashboard_host,
        "--server.port",
        dashboard_port,
        "--server.headless",
        "true",
    ]

    dashboard_env = os.environ.copy()
    dashboard_env.setdefault("MACHINOCARE_API_URL", f"http://127.0.0.1:{backend_port}")

    backend = subprocess.Popen(backend_cmd, cwd=ROOT)
    dashboard = subprocess.Popen(dashboard_cmd, cwd=ROOT, env=dashboard_env)
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

    def _shutdown(exit_code: int) -> None:
        stop_process(backend_proc)
        stop_process(dashboard_proc)
        raise SystemExit(exit_code)

    def _handle_signal(signum: int, frame: object) -> None:
        del signum, frame
        _shutdown(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    while True:
        backend_code = backend_proc.poll()
        dashboard_code = dashboard_proc.poll()

        if backend_code is not None or dashboard_code is not None:
            if backend_code is None:
                stop_process(backend_proc)
                backend_code = backend_proc.poll()
            if dashboard_code is None:
                stop_process(dashboard_proc)
                dashboard_code = dashboard_proc.poll()

            _shutdown(max(backend_code or 0, dashboard_code or 0))

        time.sleep(0.5)
