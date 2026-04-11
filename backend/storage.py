from __future__ import annotations

import json
import sqlite3
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


class DataStore:
    """Hybrid storage: in-memory for live reads + SQLite durability."""

    def __init__(self, db_path: str, max_buffer_size: int = 6000) -> None:
        self.db_path = db_path
        self.max_buffer_size = max_buffer_size
        self.buffers: dict[str, deque[dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self.max_buffer_size)
        )
        self.machine_states: dict[str, dict[str, Any]] = {}
        self.model_packages: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

        db_parent = Path(db_path).parent
        db_parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._init_db()

    @staticmethod
    def _device_key(machine_id: str, device_id: str) -> str:
        return f"{machine_id}::{device_id}"

    def _init_db(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stream_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    machine_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    acc_mag REAL NOT NULL,
                    gyro_mag REAL NOT NULL,
                    gx REAL NOT NULL,
                    gy REAL NOT NULL,
                    gz REAL NOT NULL,
                    sw420 INTEGER,
                    sequence INTEGER
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS calibrations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    machine_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    baseline_mean_acc REAL NOT NULL,
                    baseline_std_acc REAL NOT NULL,
                    threshold_mean_3sigma REAL NOT NULL,
                    package_json TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS anomaly_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    machine_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    acc_mag REAL NOT NULL,
                    score REAL,
                    threshold REAL,
                    reason TEXT
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_stream_machine_time ON stream_samples(machine_id, timestamp)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cal_machine_time ON calibrations(machine_id, created_at)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_anom_machine_time ON anomaly_events(machine_id, timestamp)"
            )

    def add_samples(
        self,
        machine_id: str,
        device_id: str,
        samples: list[dict[str, Any]],
    ) -> None:
        rows = []
        with self._lock:
            buffer = self.buffers[machine_id]
            for sample in samples:
                record = dict(sample)
                record["machine_id"] = machine_id
                record["device_id"] = device_id
                buffer.append(record)
                rows.append(
                    (
                        machine_id,
                        device_id,
                        record["timestamp"],
                        record["acc_mag"],
                        record.get("gyro_mag", 0.0),
                        record.get("gx", 0.0),
                        record.get("gy", 0.0),
                        record.get("gz", 0.0),
                        record.get("sw420"),
                        record.get("sequence"),
                    )
                )

            with self.conn:
                self.conn.executemany(
                    """
                    INSERT INTO stream_samples (
                        machine_id, device_id, timestamp, acc_mag, gyro_mag, gx, gy, gz, sw420, sequence
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )

    def get_recent_samples(
        self,
        machine_id: str,
        device_id: str | None = None,
        *,
        limit: int = 500,
        seconds: int | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            buffered = list(self.buffers.get(machine_id, []))

        if not buffered:
            return self._recent_samples_from_db(machine_id, device_id=device_id, limit=limit, seconds=seconds)

        if device_id is not None:
            buffered = [sample for sample in buffered if sample.get("device_id") == device_id]

        if seconds is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=seconds)
            filtered = [
                sample
                for sample in buffered
                if datetime.fromisoformat(sample["timestamp"]) >= cutoff
            ]
        else:
            filtered = buffered

        return filtered[-limit:]

    def _recent_samples_from_db(
        self,
        machine_id: str,
        device_id: str | None,
        *,
        limit: int,
        seconds: int | None,
    ) -> list[dict[str, Any]]:
        params: list[Any] = [machine_id]
        where = "machine_id = ?"
        if device_id is not None:
            where += " AND device_id = ?"
            params.append(device_id)
        if seconds is not None:
            cutoff = (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()
            where += " AND timestamp >= ?"
            params.append(cutoff)

        params.append(limit)
        query = f"""
            SELECT timestamp, acc_mag, gyro_mag, gx, gy, gz, sw420, sequence
            FROM stream_samples
            WHERE {where}
            ORDER BY timestamp DESC
            LIMIT ?
        """

        rows = self.conn.execute(query, params).fetchall()
        rows = list(reversed(rows))
        return [dict(row) for row in rows]

    def get_latest_sample(self, machine_id: str) -> dict[str, Any] | None:
        with self._lock:
            buffer = self.buffers.get(machine_id)
            if buffer:
                return dict(buffer[-1])

        return self.get_latest_sample_for_device(machine_id=machine_id, device_id=None)

    def get_latest_sample_for_device(
        self,
        machine_id: str,
        device_id: str | None,
    ) -> dict[str, Any] | None:
        params: list[Any] = [machine_id]
        where = "machine_id = ?"
        if device_id is not None:
            where += " AND device_id = ?"
            params.append(device_id)

        row = self.conn.execute(
            f"""
            SELECT timestamp, acc_mag, gyro_mag, gx, gy, gz, sw420, sequence, machine_id, device_id
            FROM stream_samples
            WHERE {where}
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            params,
        ).fetchone()
        return dict(row) if row else None

    def save_model_package(
        self,
        machine_id: str,
        device_id: str,
        package: dict[str, Any],
        baseline_stats: dict[str, float],
    ) -> None:
        with self._lock:
            self.model_packages[self._device_key(machine_id, device_id)] = dict(package)
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO calibrations (
                        machine_id,
                        device_id,
                        created_at,
                        baseline_mean_acc,
                        baseline_std_acc,
                        threshold_mean_3sigma,
                        package_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        machine_id,
                        device_id,
                        package["created_at"],
                        baseline_stats["mean_acc"],
                        baseline_stats["std_acc"],
                        baseline_stats["threshold_mean_3sigma"],
                        json.dumps(package, separators=(",", ":")),
                    ),
                )

    def get_model_package(self, machine_id: str, device_id: str | None = None) -> dict[str, Any] | None:
        if device_id is not None:
            return self.get_model_package_for_device(machine_id, device_id)

        # Backward-compatible: latest model package for a machine.
        row = self.conn.execute(
            """
            SELECT package_json
            FROM calibrations
            WHERE machine_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (machine_id,),
        ).fetchone()
        if not row:
            return None

        package = json.loads(row["package_json"])
        return dict(package)

    def get_model_package_for_device(self, machine_id: str, device_id: str) -> dict[str, Any] | None:
        key = self._device_key(machine_id, device_id)
        with self._lock:
            package = self.model_packages.get(key)
            if package:
                return dict(package)

        row = self.conn.execute(
            """
            SELECT package_json
            FROM calibrations
            WHERE machine_id = ? AND device_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (machine_id, device_id),
        ).fetchone()
        if not row:
            return None

        package = json.loads(row["package_json"])
        with self._lock:
            self.model_packages[key] = package
        return dict(package)

    def set_machine_state(self, machine_id: str, device_id: str, state: dict[str, Any]) -> None:
        with self._lock:
            self.machine_states[self._device_key(machine_id, device_id)] = dict(state)

    def get_machine_state(self, machine_id: str, device_id: str | None = None) -> dict[str, Any]:
        if device_id is not None:
            with self._lock:
                return dict(self.machine_states.get(self._device_key(machine_id, device_id), {}))

        latest_device = self.latest_device_for_machine(machine_id)
        if latest_device:
            with self._lock:
                key = self._device_key(machine_id, latest_device)
                return dict(self.machine_states.get(key, {}))

        with self._lock:
            return {}

    def record_anomaly(
        self,
        machine_id: str,
        device_id: str,
        timestamp: str,
        acc_mag: float,
        score: float | None,
        threshold: float | None,
        reason: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO anomaly_events (
                    machine_id, device_id, timestamp, acc_mag, score, threshold, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (machine_id, device_id, timestamp, acc_mag, score, threshold, reason),
            )

    def get_anomalies(
        self,
        machine_id: str,
        device_id: str | None = None,
        *,
        hours: int = 24,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        params: list[Any] = [machine_id, cutoff]
        where = "machine_id = ? AND timestamp >= ?"
        if device_id is not None:
            where += " AND device_id = ?"
            params.append(device_id)
        params.append(limit)

        rows = self.conn.execute(
            f"""
            SELECT timestamp, acc_mag, score, threshold, reason
            FROM anomaly_events
            WHERE {where}
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    def list_machine_ids(self) -> list[str]:
        with self._lock:
            known = set(self.buffers.keys())

            for key in self.machine_states.keys():
                machine = key.split("::", 1)[0]
                known.add(machine)

        rows = self.conn.execute("SELECT DISTINCT machine_id FROM stream_samples").fetchall()
        known.update(row[0] for row in rows)
        return sorted(known)

    def list_devices(self, machine_id: str) -> list[str]:
        devices = set()

        with self._lock:
            for sample in self.buffers.get(machine_id, []):
                device = sample.get("device_id")
                if device:
                    devices.add(device)

            for key in self.machine_states.keys():
                if key.startswith(f"{machine_id}::"):
                    devices.add(key.split("::", 1)[1])

        rows = self.conn.execute(
            "SELECT DISTINCT device_id FROM stream_samples WHERE machine_id = ?",
            (machine_id,),
        ).fetchall()
        devices.update(row[0] for row in rows)

        rows = self.conn.execute(
            "SELECT DISTINCT device_id FROM calibrations WHERE machine_id = ?",
            (machine_id,),
        ).fetchall()
        devices.update(row[0] for row in rows)

        return sorted(d for d in devices if d)

    def latest_device_for_machine(self, machine_id: str) -> str | None:
        with self._lock:
            buffer = self.buffers.get(machine_id)
            if buffer:
                for sample in reversed(buffer):
                    device = sample.get("device_id")
                    if device:
                        return str(device)

        row = self.conn.execute(
            """
            SELECT device_id
            FROM stream_samples
            WHERE machine_id = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (machine_id,),
        ).fetchone()
        if row and row[0]:
            return str(row[0])

        row = self.conn.execute(
            """
            SELECT device_id
            FROM calibrations
            WHERE machine_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (machine_id,),
        ).fetchone()
        if row and row[0]:
            return str(row[0])

        return None
