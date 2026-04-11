from __future__ import annotations

import hashlib
import json
from typing import Mapping, Sequence

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import Ridge

FEATURE_ORDER = [
    "mean_acc_mag",
    "std_acc_mag",
    "max_acc_mag",
    "mean_gyro_mag",
    "std_gx",
    "std_gy",
    "std_gz",
    "axis_imbalance_ratio",
]


def _values(window: Sequence[Mapping[str, float]], key: str) -> np.ndarray:
    return np.array([float(sample.get(key, 0.0)) for sample in window], dtype=float)


def _std(values: np.ndarray) -> float:
    if values.size <= 1:
        return 0.0
    return float(np.std(values))


def extract_feature_vector(window: Sequence[Mapping[str, float]]) -> np.ndarray:
    """Build the compact 8-feature vector used by backend and ESP32."""
    if not window:
        raise ValueError("Cannot extract features from an empty window.")

    acc = _values(window, "acc_mag")
    gyro_mag = _values(window, "gyro_mag")
    gx = _values(window, "gx")
    gy = _values(window, "gy")
    gz = _values(window, "gz")

    std_gx = _std(gx)
    std_gy = _std(gy)
    std_gz = _std(gz)
    axis_sum = std_gx + std_gy + std_gz
    axis_imbalance_ratio = max(std_gx, std_gy, std_gz) / (axis_sum + 1e-6)

    return np.array(
        [
            float(np.mean(acc)),
            _std(acc),
            float(np.max(acc)),
            float(np.mean(gyro_mag)),
            std_gx,
            std_gy,
            std_gz,
            float(axis_imbalance_ratio),
        ],
        dtype=float,
    )


def build_feature_matrix(
    samples: Sequence[Mapping[str, float]],
    window_size: int,
) -> np.ndarray:
    if not samples:
        return np.empty((0, len(FEATURE_ORDER)))

    if window_size <= 1:
        window_size = 2

    if len(samples) < window_size:
        windows = [samples]
    else:
        step = max(1, window_size // 2)
        windows = [
            samples[start : start + window_size]
            for start in range(0, len(samples) - window_size + 1, step)
        ]

    feature_rows = [extract_feature_vector(window) for window in windows if window]
    if not feature_rows:
        return np.empty((0, len(FEATURE_ORDER)))
    return np.vstack(feature_rows)


def train_isolation_forest_distilled(
    feature_matrix: np.ndarray,
    contamination: float,
) -> dict:
    if feature_matrix.shape[0] < 8:
        raise ValueError("Need at least 8 windows for calibration.")

    feature_means = np.mean(feature_matrix, axis=0)
    feature_stds = np.std(feature_matrix, axis=0)
    feature_stds = np.where(feature_stds < 1e-6, 1.0, feature_stds)

    normalized = (feature_matrix - feature_means) / feature_stds

    iso = IsolationForest(
        contamination=contamination,
        random_state=42,
        n_estimators=120,
    )
    iso.fit(feature_matrix)
    iso_scores = -iso.score_samples(feature_matrix)

    distill = Ridge(alpha=1.0)
    distill.fit(normalized, iso_scores)
    distilled_scores = distill.predict(normalized)

    high = float(np.percentile(distilled_scores, 95))
    low = float(np.percentile(distilled_scores, 90))

    if np.std(iso_scores) < 1e-8 or np.std(distilled_scores) < 1e-8:
        quality = 0.0
    else:
        quality = float(np.corrcoef(iso_scores, distilled_scores)[0, 1])

    return {
        "feature_means": feature_means.tolist(),
        "feature_stds": feature_stds.tolist(),
        "weights": distill.coef_.tolist(),
        "bias": float(distill.intercept_),
        "decision_threshold": high,
        "hysteresis_high": high,
        "hysteresis_low": low,
        "quality_correlation": quality,
        "window_count": int(feature_matrix.shape[0]),
    }


def score_feature_vector(feature_vector: np.ndarray, package: Mapping[str, object]) -> float:
    means = np.array(package["feature_means"], dtype=float)
    stds = np.array(package["feature_stds"], dtype=float)
    stds = np.where(stds < 1e-6, 1.0, stds)
    weights = np.array(package["weights"], dtype=float)
    bias = float(package["bias"])

    z = (feature_vector - means) / stds
    return float(np.dot(weights, z) + bias)


def latest_feature_vector(
    samples: Sequence[Mapping[str, float]],
    window_size: int,
) -> np.ndarray | None:
    if not samples:
        return None
    subset = samples[-window_size:] if len(samples) >= window_size else samples
    if not subset:
        return None
    return extract_feature_vector(subset)


def acc_threshold_stats(samples: Sequence[Mapping[str, float]]) -> dict:
    acc = np.array([float(sample.get("acc_mag", 0.0)) for sample in samples], dtype=float)
    mean_acc = float(np.mean(acc))
    std_acc = float(np.std(acc))
    threshold = float(mean_acc + (3.0 * std_acc))
    return {
        "mean_acc": mean_acc,
        "std_acc": std_acc,
        "threshold_mean_3sigma": threshold,
    }


def build_checksum(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
