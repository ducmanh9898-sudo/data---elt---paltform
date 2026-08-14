import os
from pathlib import Path

import pandas as pd
import streamlit as st
from trino.dbapi import connect

# -------------------------------------------------------------------
# Page configuration
# -------------------------------------------------------------------

st.set_page_config(
    page_title="Environmental Data Platform",
    page_icon="🌍",
    layout="wide",
)


# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

APP_DIR = Path(__file__).resolve().parent
DATA_PATH = APP_DIR / "data" / "gold_city_environment_hourly.csv"

# -------------------------------------------------------------------
# Realtime serving configuration
# -------------------------------------------------------------------

TRINO_HOST = os.getenv(
    "TRINO_HOST",
    "localhost",
)

TRINO_PORT = int(
    os.getenv(
        "TRINO_PORT",
        "8085",
    )
)

TRINO_USER = os.getenv(
    "TRINO_USER",
    "streamlit",
)

TRINO_CATALOG = os.getenv(
    "TRINO_CATALOG",
    "iceberg",
)

TRINO_SCHEMA = os.getenv(
    "TRINO_SCHEMA",
    "realtime",
)

TRINO_HTTP_SCHEME = os.getenv(
    "TRINO_HTTP_SCHEME",
    "http",
)
# -------------------------------------------------------------------
# Data loading
# -------------------------------------------------------------------
@st.fragment(
    run_every="5s",
)
def render_realtime_dashboard() -> None:

    st.subheader(
        "Native Streaming Sensor Dashboard"
    )
    st.markdown(
    """
    This dashboard demonstrates two serving modes:

    - **Analytics** — curated historical data from the Gold lakehouse layer.
    - **Realtime** — live IoT sensor data processed by Kafka and Flink.
    """
)
    st.caption(
        "Kafka → Flink → Iceberg → Trino realtime serving views"
    )

    # ---------------------------------------------------------------
    # Pipeline status
    # ---------------------------------------------------------------

    try:
        status_df = query_realtime_view(
            """
            SELECT
                latest_event_at,
                latest_processed_at,
                seconds_since_last_processed,
                events_last_1m,
                events_last_5m,
                active_cities_last_5m,
                is_live
            FROM live_sensor_status
            """
        )

    except Exception:
        st.info(
            "○ DEMO BACKEND OFFLINE"
        )

        st.caption(
            "The public dashboard is available, but the "
            "live streaming infrastructure is not currently "
            "reachable. Historical Analytics remains available."
        )

        metric_1, metric_2, metric_3 = st.columns(3)

        metric_1.metric(
            "Events / min",
            "—",
        )

        metric_2.metric(
            "Active Cities",
            "—",
        )

        metric_3.metric(
            "Last Update",
            "—",
        )

        st.markdown(
            """
            **Realtime architecture**

            `IoT Simulator → Kafka → Flink → Iceberg → Trino → Streamlit`

            Start the local streaming infrastructure to activate
            this dashboard.
            """
        )

        return

    if status_df.empty:
        st.warning(
            "○ REALTIME STATUS UNAVAILABLE"
        )
        return

    status = status_df.iloc[0]

    is_live = bool(
        status["is_live"]
    )

    seconds_since_processed = int(
        status[
            "seconds_since_last_processed"
        ]
    )

    events_last_1m = int(
        status[
            "events_last_1m"
        ]
    )

    events_last_5m = int(
        status[
            "events_last_5m"
        ]
    )

    active_cities = int(
        status[
            "active_cities_last_5m"
        ]
    )

    if is_live:
        st.success(
            "● LIVE — Native streaming pipeline active"
        )

    else:
        st.warning(
            "○ PIPELINE OFFLINE / IDLE"
        )

        st.caption(
            "The serving backend is reachable, but no sensor "
            "event has been processed within the live threshold."
        )

    # ---------------------------------------------------------------
    # KPIs
    # ---------------------------------------------------------------

    metric_1, metric_2, metric_3, metric_4 = st.columns(4)

    metric_1.metric(
        "Events / min",
        f"{events_last_1m:,}",
    )

    metric_2.metric(
        "Events / 5 min",
        f"{events_last_5m:,}",
    )

    metric_3.metric(
        "Active Cities",
        f"{active_cities:,}",
    )

    metric_4.metric(
        "Last Update",
        (
            f"{seconds_since_processed}s ago"
        ),
    )

    st.divider()

    # ---------------------------------------------------------------
    # Latest state by city
    # ---------------------------------------------------------------

    try:
        latest_df = query_realtime_view(
            """
            SELECT
                city_id,
                city_name,
                country_code,
                pm2_5,
                pm10,
                temperature_2m,
                relative_humidity_2m,
                event_time_utc,
                seconds_since_processed,
                is_live
            FROM live_sensor_latest_by_city
            ORDER BY city_name
            """
        )

    except Exception:
        st.warning(
            "Unable to load latest city sensor readings."
        )
        return

    st.markdown(
        "#### Latest Sensor Readings"
    )

    if latest_df.empty:
        st.info(
            "No sensor readings are currently available."
        )
        return

    display_latest_df = latest_df[
        [
            "city_name",
            "country_code",
            "pm2_5",
            "pm10",
            "temperature_2m",
            "relative_humidity_2m",
            "seconds_since_processed",
        ]
    ].copy()

    display_latest_df = display_latest_df.rename(
        columns={
            "city_name":
                "City",
            "country_code":
                "Country",
            "pm2_5":
                "PM2.5",
            "pm10":
                "PM10",
            "temperature_2m":
                "Temperature",
            "relative_humidity_2m":
                "Humidity",
            "seconds_since_processed":
                "Seconds Since Update",
        }
    )

    st.dataframe(
        display_latest_df,
        width="stretch",
        hide_index=True,
    )

    st.divider()

    # ---------------------------------------------------------------
    # Live time series
    # ---------------------------------------------------------------

    city_options = sorted(
        latest_df[
            "city_name"
        ]
        .dropna()
        .unique()
        .tolist()
    )

    if not city_options:
        return

    selected_live_city = st.selectbox(
        "Realtime city",
        options=city_options,
        key="realtime_city",
    )

    try:
        timeseries_df = query_realtime_view(
            """
            SELECT
                minute_utc,
                city_name,
                sensor_event_count,
                avg_pm2_5,
                avg_pm10,
                avg_temperature_2m,
                avg_relative_humidity_2m,
                data_updated_at_utc
            FROM live_sensor_timeseries_1min
            ORDER BY minute_utc
            """
        )

    except Exception:
        st.warning(
            "Unable to load realtime time-series data."
        )
        return

    city_timeseries_df = timeseries_df[
        timeseries_df[
            "city_name"
        ]
        == selected_live_city
    ].copy()

    if city_timeseries_df.empty:
        st.info(
            "No observations exist for this city "
            "within the current 60-minute realtime window."
        )
        return

    city_timeseries_df[
        "minute_utc"
    ] = pd.to_datetime(
        city_timeseries_df[
            "minute_utc"
        ],
        utc=True,
        errors="coerce",
    )

    city_timeseries_df = (
        city_timeseries_df
        .sort_values(
            "minute_utc"
        )
    )

    st.markdown(
        f"#### PM2.5 / PM10 — {selected_live_city}"
    )

    pollutant_chart_df = (
        city_timeseries_df[
            [
                "minute_utc",
                "avg_pm2_5",
                "avg_pm10",
            ]
        ]
        .set_index(
            "minute_utc"
        )
        .rename(
            columns={
                "avg_pm2_5":
                    "PM2.5",
                "avg_pm10":
                    "PM10",
            }
        )
    )

    st.line_chart(
        pollutant_chart_df,
        height=380,
    )

    st.caption(
        "Realtime panel refreshes every 5 seconds. "
        "The streaming ingestion and processing path is "
        "Kafka + Flink; the dashboard reads the serving "
        "layer through Trino."
    )
@st.cache_data
def load_analytics_data() -> pd.DataFrame:
    df = pd.read_csv(DATA_PATH)

    timestamp_columns = [
        "measured_at_utc",
        "first_sensor_event_at",
        "last_sensor_event_at",
        "sensor_data_updated_at",
        "data_updated_at_utc",
    ]

    for column in timestamp_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column],
                utc=True,
                errors="coerce",
            )

    numeric_columns = [
        "city_id",
        "pm2_5",
        "pm10",
        "temperature_2m",
        "relative_humidity_2m",
        "sensor_event_count",
        "sensor_device_count",
        "sensor_avg_pm2_5",
        "sensor_avg_pm10",
        "sensor_avg_temperature_2m",
        "sensor_avg_relative_humidity_2m",
        "pm2_5_sensor_minus_batch",
        "pm10_sensor_minus_batch",
        "temperature_2m_sensor_minus_batch",
        "relative_humidity_2m_sensor_minus_batch",
    ]

    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

    if "has_sensor_data" in df.columns:
        df["has_sensor_data"] = (
            df["has_sensor_data"]
            .astype(str)
            .str.strip()
            .str.lower()
            .eq("true")
        )

    return df
def query_realtime_view(
    query: str,
) -> pd.DataFrame:
    """
    Execute a read-only query against the Trino realtime
    serving schema.

    Connection failures are handled by the dashboard so
    the public Analytics experience remains available.
    """

    connection = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme=TRINO_HTTP_SCHEME,
    )

    try:
        cursor = connection.cursor()
        cursor.execute(query)

        rows = cursor.fetchall()

        columns = [
            description[0]
            for description in cursor.description
        ]

        return pd.DataFrame(
            rows,
            columns=columns,
        )

    finally:
        connection.close()

# -------------------------------------------------------------------
# Header
# -------------------------------------------------------------------

st.title("🌍 Environmental Data Platform")

st.caption(
    "Batch + Native Streaming Lakehouse | "
    "Public Data Engineering Demo"
)

st.markdown(
    """
    This dashboard demonstrates the serving layer of an environmental
    data platform combining batch API data with native IoT streaming.
    """
)


# -------------------------------------------------------------------
# Load snapshot
# -------------------------------------------------------------------

try:
    analytics_df = load_analytics_data()

except FileNotFoundError:
    st.error(
        "Analytics snapshot was not found at: "
        f"`{DATA_PATH}`"
    )
    st.stop()

except Exception as exc:
    st.error(
        "Unable to load the analytics snapshot."
    )
    st.exception(exc)
    st.stop()


# -------------------------------------------------------------------
# Navigation
# -------------------------------------------------------------------

analytics_tab, realtime_tab = st.tabs(
    [
        "📊 Analytics",
        "🔴 Realtime",
    ]
)


# ===================================================================
# ANALYTICS
# ===================================================================

with analytics_tab:

    st.subheader("Integrated Environmental Analytics")

    st.caption(
        "Curated snapshot generated from the Gold lakehouse serving layer."
    )

    # ---------------------------------------------------------------
    # Snapshot period
    # ---------------------------------------------------------------

    analytics_df["analytics_date"] = (
        analytics_df["measured_at_utc"].dt.date
    )

    min_date = analytics_df["analytics_date"].min()
    max_date = analytics_df["analytics_date"].max()

    st.caption(
        f"Snapshot period: {min_date} → {max_date}"
    )

    # ---------------------------------------------------------------
    # Filters
    # ---------------------------------------------------------------

    st.markdown("#### Filters")

    filter_1, filter_2, filter_3 = st.columns(
        [1, 2, 2]
    )

    country_options = sorted(
        analytics_df["country_code"]
        .dropna()
        .unique()
        .tolist()
    )

    city_options = sorted(
        analytics_df["city_name"]
        .dropna()
        .unique()
        .tolist()
    )

    with filter_1:
        selected_countries = st.multiselect(
            "Country",
            options=country_options,
            default=country_options,
        )

    with filter_2:
        selected_cities = st.multiselect(
            "City",
            options=city_options,
            default=city_options,
        )

    with filter_3:
        selected_dates = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

    if (
        isinstance(selected_dates, (tuple, list))
        and len(selected_dates) == 2
    ):
        start_date, end_date = selected_dates
    else:
        start_date = min_date
        end_date = max_date

    filtered_df = analytics_df[
        analytics_df["country_code"].isin(
            selected_countries
        )
        & analytics_df["city_name"].isin(
            selected_cities
        )
        & (
            analytics_df["analytics_date"]
            >= start_date
        )
        & (
            analytics_df["analytics_date"]
            <= end_date
        )
    ].copy()

    if filtered_df.empty:

        st.warning(
            "No analytics data matches the selected filters."
        )

    else:

        # -----------------------------------------------------------
        # KPI cards
        # -----------------------------------------------------------

        total_city_hours = len(filtered_df)

        total_cities = filtered_df[
            "city_id"
        ].nunique()

        integrated_sensor_hours = int(
            filtered_df[
                "has_sensor_data"
            ].sum()
        )

        integrated_sensor_events = int(
            filtered_df[
                "sensor_event_count"
            ]
            .fillna(0)
            .sum()
        )

        sensor_coverage_pct = (
            integrated_sensor_hours
            / total_city_hours
            * 100
        )

        st.markdown("#### Platform Snapshot")

        kpi_1, kpi_2, kpi_3, kpi_4 = st.columns(4)

        kpi_1.metric(
            "Cities",
            f"{total_cities:,}",
        )

        kpi_2.metric(
            "City-Hours",
            f"{total_city_hours:,}",
        )

        kpi_3.metric(
            "Integrated Sensor Hours",
            f"{integrated_sensor_hours:,}",
            help=(
                "Batch-backed Gold city-hours that also "
                "contain IoT sensor observations."
            ),
        )

        kpi_4.metric(
            "Sensor Events",
            f"{integrated_sensor_events:,}",
            help=(
                "Sensor events represented inside the "
                "selected integrated Gold city-hours."
            ),
        )

        st.caption(
            "Sensor coverage in current selection: "
            f"{sensor_coverage_pct:.2f}%"
        )

        st.divider()

        # -----------------------------------------------------------
        # PM2.5 Batch vs Sensor time series
        # -----------------------------------------------------------

        st.markdown(
            "#### PM2.5 — Batch vs IoT Sensor"
        )

        st.caption(
            "Hourly average across the currently selected cities. "
            "Sensor values appear only where streaming observations "
            "overlap the batch-backed Gold grain."
        )

        pm25_timeseries = (
            filtered_df
            .groupby(
                "measured_at_utc",
                as_index=False,
            )
            .agg(
                batch_pm2_5=(
                    "pm2_5",
                    "mean",
                ),
                sensor_pm2_5=(
                    "sensor_avg_pm2_5",
                    "mean",
                ),
            )
            .sort_values(
                "measured_at_utc"
            )
        )

        pm25_timeseries = (
            pm25_timeseries
            .set_index(
                "measured_at_utc"
            )
            .rename(
                columns={
                    "batch_pm2_5":
                        "Batch PM2.5",
                    "sensor_pm2_5":
                        "IoT Sensor PM2.5",
                }
            )
        )

        st.line_chart(
            pm25_timeseries,
            height=380,
        )

        st.divider()

        # -----------------------------------------------------------
        # Coverage + difference charts
        # -----------------------------------------------------------

        chart_left, chart_right = st.columns(2)

        with chart_left:

            st.markdown(
                "#### Sensor Coverage by City"
            )

            coverage_by_city = (
                filtered_df
                .groupby(
                    "city_name",
                    as_index=False,
                )
                .agg(
                    city_hours=(
                        "measured_at_utc",
                        "size",
                    ),
                    sensor_hours=(
                        "has_sensor_data",
                        "sum",
                    ),
                )
            )

            coverage_by_city[
                "coverage_pct"
            ] = (
                coverage_by_city[
                    "sensor_hours"
                ]
                / coverage_by_city[
                    "city_hours"
                ]
                * 100
            )

            coverage_chart = (
                coverage_by_city[
                    [
                        "city_name",
                        "coverage_pct",
                    ]
                ]
                .sort_values(
                    "coverage_pct",
                    ascending=False,
                )
                .set_index(
                    "city_name"
                )
                .rename(
                    columns={
                        "coverage_pct":
                            "Sensor Coverage (%)"
                    }
                )
            )

            st.bar_chart(
                coverage_chart,
                height=350,
            )

        # -----------------------------------------------------------
        # Sensor difference
        # -----------------------------------------------------------

        sensor_df = filtered_df[
            filtered_df[
                "has_sensor_data"
            ]
        ].copy()

        with chart_right:

            st.markdown(
                "#### Avg |PM2.5 Sensor − Batch|"
            )

            if sensor_df.empty:

                st.info(
                    "No overlapping sensor data exists "
                    "for this filter selection."
                )

            else:

                sensor_df[
                    "abs_pm2_5_difference"
                ] = (
                    sensor_df[
                        "pm2_5_sensor_minus_batch"
                    ].abs()
                )

                difference_by_city = (
                    sensor_df
                    .groupby(
                        "city_name",
                        as_index=False,
                    )
                    .agg(
                        avg_abs_difference=(
                            "abs_pm2_5_difference",
                            "mean",
                        )
                    )
                    .sort_values(
                        "avg_abs_difference",
                        ascending=False,
                    )
                )

                difference_chart = (
                    difference_by_city
                    .set_index(
                        "city_name"
                    )
                    .rename(
                        columns={
                            "avg_abs_difference":
                                "Avg absolute difference"
                        }
                    )
                )

                st.bar_chart(
                    difference_chart,
                    height=350,
                )

        st.divider()

        # -----------------------------------------------------------
        # Largest divergences
        # -----------------------------------------------------------

        st.markdown(
            "#### Largest PM2.5 Batch vs Sensor Divergences"
        )

        st.caption(
            "Sensor minus batch is a comparison metric, "
            "not an error measurement. The batch API is "
            "not treated as ground truth."
        )

        if sensor_df.empty:

            st.info(
                "No integrated sensor observations "
                "exist for this filter selection."
            )

        else:

            divergence_table = sensor_df[
                [
                    "measured_at_utc",
                    "city_name",
                    "country_code",
                    "pm2_5",
                    "sensor_avg_pm2_5",
                    "pm2_5_sensor_minus_batch",
                    "sensor_event_count",
                ]
            ].copy()

            divergence_table[
                "absolute_difference"
            ] = (
                divergence_table[
                    "pm2_5_sensor_minus_batch"
                ].abs()
            )

            divergence_table = (
                divergence_table
                .sort_values(
                    "absolute_difference",
                    ascending=False,
                )
                .head(20)
            )

            numeric_display_columns = [
                "pm2_5",
                "sensor_avg_pm2_5",
                "pm2_5_sensor_minus_batch",
                "absolute_difference",
            ]

            divergence_table[
                numeric_display_columns
            ] = divergence_table[
                numeric_display_columns
            ].round(2)

            divergence_table = (
                divergence_table
                .rename(
                    columns={
                        "measured_at_utc":
                            "Hour (UTC)",
                        "city_name":
                            "City",
                        "country_code":
                            "Country",
                        "pm2_5":
                            "Batch PM2.5",
                        "sensor_avg_pm2_5":
                            "Sensor PM2.5",
                        "pm2_5_sensor_minus_batch":
                            "Sensor − Batch",
                        "absolute_difference":
                            "|Difference|",
                        "sensor_event_count":
                            "Sensor Events",
                    }
                )
            )

            st.dataframe(
                divergence_table,
                width="stretch",
                hide_index=True,
            )


# ===================================================================
# REALTIME
# ===================================================================

with realtime_tab:

    render_realtime_dashboard()

st.divider()

st.caption(
    "Data Engineering Demo · "
    "Airflow · Kafka · Flink · Spark · Iceberg · dbt · Trino · Streamlit"
)