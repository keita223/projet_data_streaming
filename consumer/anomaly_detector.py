"""
Anomaly Detector — Isolation Forest
Detects unusual weather patterns in streaming data.

Features used:
  temperature, humidity, pressure, wind_speed, clouds

An anomaly is flagged when the Isolation Forest scores a record
below the contamination threshold (default 5%).
The model is persisted to disk and reloaded between batches,
providing an online-learning effect as data accumulates.
"""

import logging
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

FEATURES = ["temperature", "humidity", "pressure", "wind_speed", "clouds"]
MIN_SAMPLES = 5          # minimum records needed before running detection
CONTAMINATION = 0.05     # expected fraction of anomalies (5%)
N_ESTIMATORS = 100


class AnomalyDetector:
    """
    Isolation Forest wrapper for streaming weather anomaly detection.

    The detector follows a partial-fit strategy:
      - If a saved model exists, it is loaded and used for prediction,
        then retrained on the combined history.
      - If no model exists, it is trained on the current batch.
    """

    def __init__(self, model_dir: str = "/app/data"):
        self.model_path  = Path(model_dir) / "iso_forest.pkl"
        self.scaler_path = Path(model_dir) / "scaler.pkl"
        self.model: IsolationForest | None = None
        self.scaler: StandardScaler | None = None
        self._load()

    # ── Persistence ───────────────────────────────────────────────────────────
    def _load(self) -> None:
        if self.model_path.exists() and self.scaler_path.exists():
            try:
                self.model  = joblib.load(self.model_path)
                self.scaler = joblib.load(self.scaler_path)
                logger.info("Loaded existing Isolation Forest model from disk.")
                return
            except Exception as exc:
                logger.warning(f"Could not load saved model: {exc}")
        self.model  = IsolationForest(
            n_estimators=N_ESTIMATORS,
            contamination=CONTAMINATION,
            random_state=42,
            n_jobs=-1,
        )
        self.scaler = StandardScaler()
        logger.info("Initialized new Isolation Forest model.")

    def _save(self) -> None:
        try:
            joblib.dump(self.model,  self.model_path)
            joblib.dump(self.scaler, self.scaler_path)
        except Exception as exc:
            logger.warning(f"Could not save model: {exc}")

    # ── Core method ───────────────────────────────────────────────────────────
    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fit/retrain the model on `df` and return the rows flagged as anomalies.

        Returns:
            DataFrame with original columns plus:
              - anomaly_label  : -1 (anomaly) or 1 (normal)
              - anomaly_score  : raw Isolation Forest score (lower = more anomalous)
              - is_anomaly     : boolean convenience flag
        """
        available = [f for f in FEATURES if f in df.columns]
        if not available or len(df) < MIN_SAMPLES:
            return pd.DataFrame()

        X = df[available].copy()
        X = X.fillna(X.median(numeric_only=True))

        # Scale
        X_scaled = self.scaler.fit_transform(X)

        # Fit + predict (batch re-training — simple online approximation)
        labels = self.model.fit_predict(X_scaled)
        scores = self.model.score_samples(X_scaled)

        self._save()

        result = df.copy()
        result["anomaly_label"] = labels
        result["anomaly_score"] = np.round(scores, 4)
        result["is_anomaly"]    = labels == -1

        anomalies = result[result["is_anomaly"]].copy()

        if not anomalies.empty:
            logger.info(
                f"Anomalies: {len(anomalies)}/{len(df)} records flagged  "
                f"(scores: {scores[labels == -1].min():.3f} … {scores[labels == -1].max():.3f})"
            )
            self._log_details(anomalies, available)

        return anomalies

    # ── Diagnostics ───────────────────────────────────────────────────────────
    def _log_details(self, anomalies: pd.DataFrame, features: list[str]) -> None:
        for _, row in anomalies.iterrows():
            vals = {f: round(row[f], 1) for f in features if f in row}
            logger.info(
                f"  ⚠  {row.get('city', '?'):12s} "
                f"score={row['anomaly_score']:.3f}  {vals}"
            )

    # ── Summary statistics ────────────────────────────────────────────────────
    def feature_importance(self, df: pd.DataFrame) -> pd.Series | None:
        """
        Approximate feature importance via mean absolute deviation from city median.
        Used for dashboard display.
        """
        available = [f for f in FEATURES if f in df.columns]
        if not available or df.empty:
            return None
        deviations = df[available].apply(
            lambda col: (col - col.median()).abs().mean()
        )
        return deviations.sort_values(ascending=False)
