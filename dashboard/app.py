"""
Real-Time Weather Streaming Dashboard — Streamlit
Reads data written by the Spark consumer and displays:
  - Current conditions per city (KPI cards)
  - Temperature time series
  - Humidity & pressure charts
  - Wind speed bar chart
  - 5-minute windowed aggregations
  - ML anomaly alerts (Isolation Forest results)
Auto-refreshes every 30 seconds.
"""

import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Weather Stream Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

DATA_DIR      = Path(os.getenv("DATA_DIR", "/app/data"))
RECORDS_FILE  = DATA_DIR / "weather_records.json"
ANOMALY_FILE  = DATA_DIR / "anomalies.json"
AGG_DIR       = DATA_DIR / "aggregations"
REFRESH_SECS  = 30

CITY_FLAGS = {
    "Paris": "🇫🇷", "London": "🇬🇧", "Berlin": "🇩🇪", "Madrid": "🇪🇸",
    "Rome": "🇮🇹", "Amsterdam": "🇳🇱", "Brussels": "🇧🇪", "Vienna": "🇦🇹",
    "Zurich": "🇨🇭", "Stockholm": "🇸🇪",
}
WEATHER_ICONS = {
    "Clear": "☀️", "Clouds": "☁️", "Rain": "🌧️",
    "Snow": "❄️", "Thunderstorm": "⛈️", "Drizzle": "🌦️",
    "Mist": "🌫️", "Fog": "🌫️", "Haze": "🌫️",
}


# ─── Data loaders ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=REFRESH_SECS)
def load_records() -> pd.DataFrame:
    if not RECORDS_FILE.exists():
        return pd.DataFrame()
    try:
        data = json.loads(RECORDS_FILE.read_text())
        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        return df.sort_values("timestamp")
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_SECS)
def load_anomalies() -> pd.DataFrame:
    if not ANOMALY_FILE.exists():
        return pd.DataFrame()
    try:
        data = json.loads(ANOMALY_FILE.read_text())
        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        return df.sort_values("timestamp", ascending=False)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_SECS)
def load_aggregations() -> pd.DataFrame:
    if not AGG_DIR.exists():
        return pd.DataFrame()
    parts = []
    for f in AGG_DIR.glob("*.json"):
        try:
            parts.append(pd.read_json(f, lines=True))
        except Exception:
            pass
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    if "window_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["window_start"], utc=True, errors="coerce")
    return df.sort_values("window_start") if "window_start" in df.columns else df


# ─── UI helpers ───────────────────────────────────────────────────────────────
def weather_icon(main: str) -> str:
    return WEATHER_ICONS.get(main, "🌡️")

def delta_color(val: float, threshold: float = 0) -> str:
    return "normal" if val >= threshold else "inverse"


# ─── Layout ───────────────────────────────────────────────────────────────────
def main():
    # Auto-refresh
    st.markdown(
        f"""<meta http-equiv="refresh" content="{REFRESH_SECS}">""",
        unsafe_allow_html=True,
    )

    # ── Header ────────────────────────────────────────────────────────────────
    col_title, col_time = st.columns([3, 1])
    with col_title:
        st.title("🌤️ Weather Data Streaming Dashboard")
        st.caption("OpenWeatherMap → Kafka → Spark Structured Streaming → Isolation Forest")
    with col_time:
        st.metric("Last refresh", datetime.utcnow().strftime("%H:%M:%S UTC"))
        st.caption(f"Auto-refresh every {REFRESH_SECS}s")

    st.divider()

    # ── Load data ─────────────────────────────────────────────────────────────
    df       = load_records()
    anomalies = load_anomalies()
    agg      = load_aggregations()

    if df.empty:
        st.info(
            "⏳ Waiting for data… The pipeline is starting up.\n\n"
            "Make sure the producer and Spark consumer containers are running."
        )
        st.stop()

    # Latest record per city
    latest = df.sort_values("timestamp").groupby("city").last().reset_index()

    # ── KPI strip ─────────────────────────────────────────────────────────────
    st.subheader("Current Conditions")
    cols = st.columns(len(latest))
    for col, (_, row) in zip(cols, latest.iterrows()):
        flag = CITY_FLAGS.get(row["city"], "🏙️")
        icon = weather_icon(row.get("weather_main", ""))
        with col:
            st.metric(
                label=f"{flag} {row['city']}",
                value=f"{row['temperature']:.1f}°C",
                delta=f"{row.get('weather_description', '').capitalize()}",
            )
            st.caption(
                f"{icon}  💧{row.get('humidity', '?')}%  "
                f"🌬️{row.get('wind_speed', '?')} m/s"
            )

    st.divider()

    # ── Charts row 1 ──────────────────────────────────────────────────────────
    col_temp, col_humid = st.columns(2)

    with col_temp:
        st.subheader("🌡️ Temperature over time")
        fig = px.line(
            df,
            x="timestamp", y="temperature", color="city",
            labels={"temperature": "°C", "timestamp": "Time"},
            template="plotly_dark",
        )
        fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02))
        st.plotly_chart(fig, use_container_width=True)

    with col_humid:
        st.subheader("💧 Humidity & Pressure")
        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        cities_sample = latest["city"].tolist()[:5]
        df_last = df[df["city"].isin(cities_sample)]
        for city in cities_sample:
            d = df_last[df_last["city"] == city]
            fig2.add_trace(
                go.Scatter(x=d["timestamp"], y=d["humidity"],
                           name=f"{city} humidity", mode="lines"),
                secondary_y=False,
            )
            fig2.add_trace(
                go.Scatter(x=d["timestamp"], y=d["pressure"],
                           name=f"{city} pressure", mode="lines",
                           line=dict(dash="dot")),
                secondary_y=True,
            )
        fig2.update_yaxes(title_text="Humidity (%)", secondary_y=False)
        fig2.update_yaxes(title_text="Pressure (hPa)", secondary_y=True)
        fig2.update_layout(template="plotly_dark",
                           legend=dict(orientation="h", yanchor="bottom", y=1.02))
        st.plotly_chart(fig2, use_container_width=True)

    # ── Charts row 2 ──────────────────────────────────────────────────────────
    col_wind, col_heat = st.columns(2)

    with col_wind:
        st.subheader("🌬️ Wind Speed by City")
        fig3 = px.bar(
            latest.sort_values("wind_speed", ascending=False),
            x="city", y="wind_speed",
            color="wind_speed",
            color_continuous_scale="Blues",
            labels={"wind_speed": "m/s"},
            template="plotly_dark",
        )
        fig3.update_layout(showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

    with col_heat:
        st.subheader("🗺️ Multi-city Heatmap")
        pivot = latest.set_index("city")[
            ["temperature", "humidity", "pressure", "wind_speed", "clouds"]
        ].T
        fig4 = px.imshow(
            pivot,
            color_continuous_scale="RdBu_r",
            aspect="auto",
            template="plotly_dark",
            labels={"color": "Value"},
        )
        st.plotly_chart(fig4, use_container_width=True)

    st.divider()

    # ── Windowed aggregations ─────────────────────────────────────────────────
    st.subheader("⏱️ 5-minute Windowed Aggregations")
    if agg.empty:
        st.info("Aggregations not yet available (requires ≥5 min of data).")
    else:
        cities_agg = agg["city"].unique().tolist() if "city" in agg.columns else []
        sel_city = st.selectbox("City", options=cities_agg, key="agg_city")
        df_agg_city = agg[agg["city"] == sel_city] if sel_city else agg

        fig5 = go.Figure()
        if "window_start" in df_agg_city.columns and "avg_temp" in df_agg_city.columns:
            fig5.add_trace(go.Bar(
                x=df_agg_city["window_start"], y=df_agg_city["avg_temp"],
                name="Avg Temp", marker_color="tomato",
            ))
            fig5.add_trace(go.Scatter(
                x=df_agg_city["window_start"], y=df_agg_city["max_temp"],
                name="Max Temp", mode="lines+markers", line=dict(color="orange"),
            ))
            fig5.add_trace(go.Scatter(
                x=df_agg_city["window_start"], y=df_agg_city["min_temp"],
                name="Min Temp", mode="lines+markers", line=dict(color="lightblue"),
            ))
        fig5.update_layout(
            xaxis_title="Window start",
            yaxis_title="Temperature (°C)",
            template="plotly_dark",
            legend=dict(orientation="h"),
        )
        st.plotly_chart(fig5, use_container_width=True)

        with st.expander("Show aggregation table"):
            st.dataframe(df_agg_city.tail(20), use_container_width=True)

    st.divider()

    # ── Anomaly panel ─────────────────────────────────────────────────────────
    st.subheader("⚠️ ML Anomaly Detection — Isolation Forest")

    n_anomalies = len(anomalies)
    n_total     = len(df)
    pct         = n_anomalies / n_total * 100 if n_total else 0

    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("Total records", n_total)
    kpi2.metric("Anomalies detected", n_anomalies)
    kpi3.metric("Anomaly rate", f"{pct:.1f}%")

    if anomalies.empty:
        st.success("✅ No anomalies detected yet.")
    else:
        st.warning(f"⚠️  {n_anomalies} anomalous weather records detected.")

        # Anomaly score distribution
        if "anomaly_score" in anomalies.columns:
            fig6 = px.histogram(
                anomalies, x="anomaly_score", color="city",
                nbins=20,
                labels={"anomaly_score": "Isolation Forest score"},
                title="Anomaly score distribution",
                template="plotly_dark",
            )
            st.plotly_chart(fig6, use_container_width=True)

        # Recent anomaly table
        display_cols = [c for c in
                        ["timestamp", "city", "temperature", "humidity",
                         "pressure", "wind_speed", "weather_main", "anomaly_score"]
                        if c in anomalies.columns]
        st.dataframe(
            anomalies[display_cols].head(20).reset_index(drop=True),
            use_container_width=True,
        )

    st.divider()
    st.caption(
        "Data source: OpenWeatherMap API · "
        "Processing: Apache Spark Structured Streaming · "
        "ML: Isolation Forest (scikit-learn) · "
        "M2 IASD — Data Streaming 2025-2026"
    )


if __name__ == "__main__":
    main()
