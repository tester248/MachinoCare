from __future__ import annotations

import json
import os
import sqlite3
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - optional runtime dependency
    psycopg = None
    dict_row = None


class _CursorAdapter:
    def __init__(self, rows: list[Any] | None, rowcount: int) -> None:
        self._rows = rows or []
        self._rowcount = rowcount
        self._index = 0

    def fetchall(self) -> Any:
        if self._index >= len(self._rows):
            return []
        out = self._rows[self._index :]
        self._index = len(self._rows)
        return out

    def fetchone(self) -> Any:
        if self._index >= len(self._rows):
            return None
        item = self._rows[self._index]
        self._index += 1
        return item

    @property
    def rowcount(self) -> int:
        return int(self._rowcount or 0)


class _DbConnectionAdapter:
    def __init__(self, db_path: str, database_url: str | None = None) -> None:
        self.database_url = database_url or os.getenv("DATABASE_URL")
        self.backend = "sqlite"
        self._db_lock = threading.Lock()

        if self.database_url and self.database_url.startswith(("postgres://", "postgresql://")):
            if psycopg is None:
                raise RuntimeError(
                    "DATABASE_URL is set for PostgreSQL but psycopg is not installed. "
                    "Install psycopg[binary] to use PostgreSQL."
                )

            normalized = self.database_url.replace("postgres://", "postgresql://", 1)
            self._conn = psycopg.connect(normalized, row_factory=dict_row)
            self.backend = "postgres"
            return

        db_parent = Path(db_path).parent
        db_parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")

    def __enter__(self) -> "_DbConnectionAdapter":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        if exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()
        return False

    def _sql(self, query: str) -> str:
        if self.backend == "postgres":
            return query.replace("?", "%s")
        return query

    def execute(self, query: str, params: tuple[Any, ...] | list[Any] = ()) -> _CursorAdapter:
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute(self._sql(query), params)
            if cursor.description is None:
                rows: list[Any] = []
            else:
                rows = cursor.fetchall()
            rowcount = int(getattr(cursor, "rowcount", 0) or 0)
            cursor.close()
        return _CursorAdapter(rows, rowcount)

    def executemany(self, query: str, rows: list[tuple[Any, ...]]) -> _CursorAdapter:
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.executemany(self._sql(query), rows)
            rowcount = int(getattr(cursor, "rowcount", 0) or 0)
            cursor.close()
        return _CursorAdapter([], rowcount)


class DataStore:
    """Hybrid storage: in-memory for live reads + SQLite durability."""

    def __init__(
        self,
        db_path: str,
        max_buffer_size: int = 6000,
        database_url: str | None = None,
    ) -> None:
        self.db_path = db_path
        self.max_buffer_size = max_buffer_size
        self.buffers: dict[str, deque[dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self.max_buffer_size)
        )
        self.machine_states: dict[str, dict[str, Any]] = {}
        self.model_packages: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

        self.conn = _DbConnectionAdapter(self.db_path, database_url=database_url)
        self.backend = self.conn.backend
        self._init_db()

    @staticmethod
    def _device_key(machine_id: str, device_id: str) -> str:
        return f"{machine_id}::{device_id}"

    @staticmethod
    def _first_value(row: Any) -> Any:
        if row is None:
            return None
        if isinstance(row, dict):
            return next(iter(row.values()), None)
        return row[0]

    @staticmethod
    def normalize_device_name(value: str) -> str:
        return str(value or "").strip()

    def _init_db(self) -> None:
        if self.backend == "postgres":
            id_column = "BIGSERIAL PRIMARY KEY"
        else:
            id_column = "INTEGER PRIMARY KEY AUTOINCREMENT"

        with self.conn:
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS stream_samples (
                    id {id_column},
                    machine_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    acc_mag DOUBLE PRECISION NOT NULL,
                    gyro_mag DOUBLE PRECISION NOT NULL,
                    gx DOUBLE PRECISION NOT NULL,
                    gy DOUBLE PRECISION NOT NULL,
                    gz DOUBLE PRECISION NOT NULL,
                    sw420 INTEGER,
                    sequence INTEGER
                )
                """
            )
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS calibrations (
                    id {id_column},
                    machine_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    baseline_mean_acc DOUBLE PRECISION NOT NULL,
                    baseline_std_acc DOUBLE PRECISION NOT NULL,
                    threshold_mean_3sigma DOUBLE PRECISION NOT NULL,
                    package_json TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS anomaly_events (
                    id {id_column},
                    machine_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    acc_mag DOUBLE PRECISION NOT NULL,
                    score DOUBLE PRECISION,
                    threshold DOUBLE PRECISION,
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
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS device_profiles (
                    id {id_column},
                    machine_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    display_name TEXT,
                    sample_rate_hz INTEGER,
                    window_seconds INTEGER,
                    fallback_seconds INTEGER,
                    contamination DOUBLE PRECISION,
                    min_consecutive_windows INTEGER,
                    notes TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(machine_id, device_id)
                )
                """
            )
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS api_debug_logs (
                    id {id_column},
                    created_at TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    method TEXT NOT NULL,
                    machine_id TEXT,
                    device_id TEXT,
                    status_code INTEGER,
                    latency_ms INTEGER,
                    request_size INTEGER,
                    response_size INTEGER,
                    correlation_id TEXT,
                    is_error INTEGER NOT NULL DEFAULT 0,
                    payload_sampled INTEGER NOT NULL DEFAULT 0,
                    request_payload TEXT,
                    response_payload TEXT,
                    error_text TEXT
                )
                """
            )
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS stream_bindings (
                    id {id_column},
                    binding_name TEXT NOT NULL UNIQUE,
                    machine_id TEXT,
                    device_id TEXT,
                    source TEXT,
                    is_active INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_profiles_machine_device ON device_profiles(machine_id, device_id)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_profiles_machine ON device_profiles(machine_id)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_debug_time ON api_debug_logs(created_at)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_debug_machine_device_time ON api_debug_logs(machine_id, device_id, created_at)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_debug_endpoint_time ON api_debug_logs(endpoint, created_at)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_stream_bindings_name ON stream_bindings(binding_name)"
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

    def upsert_device_profile(self, payload: dict[str, Any]) -> dict[str, Any]:
        machine_id = str(payload["machine_id"])
        device_id = str(payload["device_id"])
        now = datetime.now(timezone.utc).isoformat()

        with self._lock:
            row = self.conn.execute(
                """
                SELECT created_at
                FROM device_profiles
                WHERE machine_id = ? AND device_id = ?
                """,
                (machine_id, device_id),
            ).fetchone()
            created_at = row["created_at"] if row else now

            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO device_profiles (
                        machine_id,
                        device_id,
                        display_name,
                        sample_rate_hz,
                        window_seconds,
                        fallback_seconds,
                        contamination,
                        min_consecutive_windows,
                        notes,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(machine_id, device_id)
                    DO UPDATE SET
                        display_name = excluded.display_name,
                        sample_rate_hz = excluded.sample_rate_hz,
                        window_seconds = excluded.window_seconds,
                        fallback_seconds = excluded.fallback_seconds,
                        contamination = excluded.contamination,
                        min_consecutive_windows = excluded.min_consecutive_windows,
                        notes = excluded.notes,
                        updated_at = excluded.updated_at
                    """,
                    (
                        machine_id,
                        device_id,
                        payload.get("display_name"),
                        payload.get("sample_rate_hz"),
                        payload.get("window_seconds"),
                        payload.get("fallback_seconds"),
                        payload.get("contamination"),
                        payload.get("min_consecutive_windows"),
                        payload.get("notes"),
                        created_at,
                        now,
                    ),
                )

        profile = self.get_device_profile(machine_id, device_id)
        return profile or {}

    def get_device_profile(self, machine_id: str, device_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT
                machine_id,
                device_id,
                display_name,
                sample_rate_hz,
                window_seconds,
                fallback_seconds,
                contamination,
                min_consecutive_windows,
                notes,
                created_at,
                updated_at
            FROM device_profiles
            WHERE machine_id = ? AND device_id = ?
            """,
            (machine_id, device_id),
        ).fetchone()
        return dict(row) if row else None

    def get_device_profile_by_name(self, device_name: str) -> dict[str, Any] | None:
        name = self.normalize_device_name(device_name)
        if not name:
            return None

        row = self.conn.execute(
            """
            SELECT
                machine_id,
                device_id,
                display_name,
                sample_rate_hz,
                window_seconds,
                fallback_seconds,
                contamination,
                min_consecutive_windows,
                notes,
                created_at,
                updated_at
            FROM device_profiles
            WHERE lower(display_name) = lower(?)
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (name,),
        ).fetchone()
        return dict(row) if row else None

    def list_device_profiles(
        self,
        machine_id: str | None = None,
        *,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if machine_id is not None:
            where = "WHERE machine_id = ?"
            params.append(machine_id)
        params.append(limit)

        rows = self.conn.execute(
            f"""
            SELECT
                machine_id,
                device_id,
                display_name,
                sample_rate_hz,
                window_seconds,
                fallback_seconds,
                contamination,
                min_consecutive_windows,
                notes,
                created_at,
                updated_at
            FROM device_profiles
            {where}
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    def delete_device_profile(self, machine_id: str, device_id: str) -> bool:
        with self.conn:
            cursor = self.conn.execute(
                "DELETE FROM device_profiles WHERE machine_id = ? AND device_id = ?",
                (machine_id, device_id),
            )

        deleted = int(cursor.rowcount or 0) > 0
        if deleted:
            binding = self.get_stream_binding()
            if (
                binding
                and binding.get("is_active")
                and binding.get("machine_id") == machine_id
                and binding.get("device_id") == device_id
            ):
                self.clear_stream_binding(source="profile_deleted")
        return deleted

    def delete_device_profile_by_name(self, device_name: str) -> dict[str, Any] | None:
        profile = self.get_device_profile_by_name(device_name)
        if not profile:
            return None

        deleted = self.delete_device_profile(str(profile["machine_id"]), str(profile["device_id"]))
        if not deleted:
            return None
        return profile

    def list_device_names(self, *, limit: int = 500) -> list[str]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT display_name
            FROM device_profiles
            WHERE display_name IS NOT NULL AND trim(display_name) <> ''
            ORDER BY lower(display_name) ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        names = [str(self._first_value(row) or "").strip() for row in rows]
        return [name for name in names if name]

    def get_stream_binding(self, binding_name: str = "primary") -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT binding_name, machine_id, device_id, source, is_active, updated_at
            FROM stream_bindings
            WHERE binding_name = ?
            LIMIT 1
            """,
            (binding_name,),
        ).fetchone()
        if not row:
            return None

        item = dict(row)
        item["is_active"] = bool(item.get("is_active"))
        return item

    def set_stream_binding(
        self,
        *,
        machine_id: str,
        device_id: str,
        source: str = "dashboard_manual",
        binding_name: str = "primary",
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO stream_bindings (
                    binding_name,
                    machine_id,
                    device_id,
                    source,
                    is_active,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(binding_name)
                DO UPDATE SET
                    machine_id = excluded.machine_id,
                    device_id = excluded.device_id,
                    source = excluded.source,
                    is_active = excluded.is_active,
                    updated_at = excluded.updated_at
                """,
                (binding_name, machine_id, device_id, source, 1, now),
            )

        binding = self.get_stream_binding(binding_name)
        return binding or {
            "binding_name": binding_name,
            "machine_id": machine_id,
            "device_id": device_id,
            "source": source,
            "is_active": True,
            "updated_at": now,
        }

    def clear_stream_binding(
        self,
        *,
        source: str = "dashboard_manual",
        binding_name: str = "primary",
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO stream_bindings (
                    binding_name,
                    machine_id,
                    device_id,
                    source,
                    is_active,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(binding_name)
                DO UPDATE SET
                    machine_id = excluded.machine_id,
                    device_id = excluded.device_id,
                    source = excluded.source,
                    is_active = excluded.is_active,
                    updated_at = excluded.updated_at
                """,
                (binding_name, None, None, source, 0, now),
            )

        binding = self.get_stream_binding(binding_name)
        return binding or {
            "binding_name": binding_name,
            "machine_id": None,
            "device_id": None,
            "source": source,
            "is_active": False,
            "updated_at": now,
        }

    @staticmethod
    def _encode_json_payload(payload: Any) -> str | None:
        if payload is None:
            return None
        if isinstance(payload, str):
            return payload
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    @staticmethod
    def _decode_json_payload(payload: str | None) -> Any:
        if payload is None:
            return None
        try:
            return json.loads(payload)
        except Exception:  # noqa: BLE001
            return payload

    def save_api_debug_log(self, entry: dict[str, Any]) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO api_debug_logs (
                    created_at,
                    endpoint,
                    method,
                    machine_id,
                    device_id,
                    status_code,
                    latency_ms,
                    request_size,
                    response_size,
                    correlation_id,
                    is_error,
                    payload_sampled,
                    request_payload,
                    response_payload,
                    error_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry.get("created_at") or datetime.now(timezone.utc).isoformat(),
                    entry.get("endpoint"),
                    entry.get("method"),
                    entry.get("machine_id"),
                    entry.get("device_id"),
                    entry.get("status_code"),
                    entry.get("latency_ms"),
                    entry.get("request_size"),
                    entry.get("response_size"),
                    entry.get("correlation_id"),
                    1 if entry.get("is_error") else 0,
                    1 if entry.get("payload_sampled") else 0,
                    self._encode_json_payload(entry.get("request_payload")),
                    self._encode_json_payload(entry.get("response_payload")),
                    entry.get("error_text"),
                ),
            )

    def list_api_debug_logs(
        self,
        *,
        machine_id: str | None = None,
        device_id: str | None = None,
        endpoint: str | None = None,
        limit: int = 300,
    ) -> list[dict[str, Any]]:
        params: list[Any] = []
        clauses: list[str] = []
        if machine_id is not None:
            clauses.append("machine_id = ?")
            params.append(machine_id)
        if device_id is not None:
            clauses.append("device_id = ?")
            params.append(device_id)
        if endpoint is not None:
            clauses.append("endpoint = ?")
            params.append(endpoint)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)

        rows = self.conn.execute(
            f"""
            SELECT
                id,
                created_at,
                endpoint,
                method,
                machine_id,
                device_id,
                status_code,
                latency_ms,
                request_size,
                response_size,
                correlation_id,
                is_error,
                payload_sampled,
                request_payload,
                response_payload,
                error_text
            FROM api_debug_logs
            {where}
            ORDER BY id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()

        decoded: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["is_error"] = bool(item.get("is_error"))
            item["payload_sampled"] = bool(item.get("payload_sampled"))
            item["request_payload"] = self._decode_json_payload(item.get("request_payload"))
            item["response_payload"] = self._decode_json_payload(item.get("response_payload"))
            decoded.append(item)

        return decoded

    def list_api_debug_logs_since(
        self,
        *,
        after_id: int,
        machine_id: str | None = None,
        device_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        params: list[Any] = [after_id]
        clauses: list[str] = ["id > ?"]
        if machine_id is not None:
            clauses.append("machine_id = ?")
            params.append(machine_id)
        if device_id is not None:
            clauses.append("device_id = ?")
            params.append(device_id)

        params.append(limit)
        where = " AND ".join(clauses)

        rows = self.conn.execute(
            f"""
            SELECT
                id,
                created_at,
                endpoint,
                method,
                machine_id,
                device_id,
                status_code,
                latency_ms,
                request_size,
                response_size,
                correlation_id,
                is_error,
                payload_sampled,
                request_payload,
                response_payload,
                error_text
            FROM api_debug_logs
            WHERE {where}
            ORDER BY id ASC
            LIMIT ?
            """,
            params,
        ).fetchall()

        decoded: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["is_error"] = bool(item.get("is_error"))
            item["payload_sampled"] = bool(item.get("payload_sampled"))
            item["request_payload"] = self._decode_json_payload(item.get("request_payload"))
            item["response_payload"] = self._decode_json_payload(item.get("response_payload"))
            decoded.append(item)

        return decoded

    def purge_api_debug_logs_older_than(self, days: int) -> int:
        if days <= 0:
            return 0
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with self.conn:
            cursor = self.conn.execute(
                "DELETE FROM api_debug_logs WHERE created_at < ?",
                (cutoff,),
            )
        return int(cursor.rowcount or 0)

    def list_machine_ids(self) -> list[str]:
        with self._lock:
            known = set(self.buffers.keys())

            for key in self.machine_states.keys():
                machine = key.split("::", 1)[0]
                known.add(machine)

        rows = self.conn.execute("SELECT DISTINCT machine_id FROM stream_samples").fetchall()
        known.update(self._first_value(row) for row in rows)

        rows = self.conn.execute(
            "SELECT DISTINCT machine_id FROM stream_bindings WHERE machine_id IS NOT NULL"
        ).fetchall()
        known.update(self._first_value(row) for row in rows)

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
        devices.update(self._first_value(row) for row in rows)

        rows = self.conn.execute(
            "SELECT DISTINCT device_id FROM calibrations WHERE machine_id = ?",
            (machine_id,),
        ).fetchall()
        devices.update(self._first_value(row) for row in rows)

        rows = self.conn.execute(
            "SELECT DISTINCT device_id FROM device_profiles WHERE machine_id = ?",
            (machine_id,),
        ).fetchall()
        devices.update(self._first_value(row) for row in rows)

        rows = self.conn.execute(
            "SELECT DISTINCT device_id FROM stream_bindings WHERE machine_id = ? AND device_id IS NOT NULL",
            (machine_id,),
        ).fetchall()
        devices.update(self._first_value(row) for row in rows)

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
        value = self._first_value(row)
        if value:
            return str(value)

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
        value = self._first_value(row)
        if value:
            return str(value)

        return None
