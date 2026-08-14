import os
from pathlib import Path

import pandas as pd
import streamlit as st
from trino.dbapi import connect
import plotly.express as px
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
DATA_PATH = APP_DIR / "data" / "batch_environment_hourly.csv"

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
        "measured_at_local",
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
    "latitude",
    "longitude",

    "pm2_5",
    "pm10",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "us_aqi",
    "air_quality_status_rank",

    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "surface_pressure",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "visibility",
    "weather_code",
]
    boolean_columns = [
        "is_air_quality_alert",
        "has_precipitation",
    ]

    for column in boolean_columns:
        if column in df.columns:
            df[column] = (
                df[column]
                .astype(str)
                .str.strip()
                .str.lower()
                .eq("true")
            )
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
def compact_figure(
    fig,
    height: int = 280,
    show_legend: bool = True,
):
    fig.update_layout(
        height=height,
        margin=dict(
            l=15,
            r=15,
            t=15,
            b=15,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        showlegend=show_legend,
    )

    return fig
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

    st.subheader("Environmental Analytics")

    st.caption(
        "Historical environmental analytics from the "
        "batch Gold lakehouse serving layer."
    )

    # ===============================================================
    # FILTER PREPARATION
    # ===============================================================

    analytics_df["analytics_date"] = (
        analytics_df["measured_at_utc"].dt.date
    )

    min_date = analytics_df["analytics_date"].min()
    max_date = analytics_df["analytics_date"].max()

    filter_1, filter_2, filter_3 = st.columns(
        [1.4, 1.6, 2]
    )

    country_lookup = (
        analytics_df[
            [
                "country_code",
                "country_name",
            ]
        ]
        .drop_duplicates()
        .sort_values("country_name")
    )

    country_names = (
        country_lookup[
            "country_name"
        ]
        .dropna()
        .tolist()
    )

    with filter_1:

        selected_country = st.selectbox(
            "Country",
            [
                "All Countries",
                *country_names,
            ],
        )

    if selected_country == "All Countries":

        country_df = analytics_df.copy()

    else:

        country_df = analytics_df[
            analytics_df["country_name"]
            == selected_country
        ].copy()

    city_names = sorted(
        country_df[
            "city_name"
        ]
        .dropna()
        .unique()
        .tolist()
    )

    with filter_2:

        selected_city = st.selectbox(
            "City",
            [
                "All Cities",
                *city_names,
            ],
        )

    with filter_3:

        selected_dates = st.date_input(
            "Date range",
            value=(
                min_date,
                max_date,
            ),
            min_value=min_date,
            max_value=max_date,
        )

    if (
        isinstance(
            selected_dates,
            (tuple, list),
        )
        and len(selected_dates) == 2
    ):
        start_date, end_date = (
            selected_dates
        )

    else:
        start_date = min_date
        end_date = max_date

    filtered_df = country_df[
        (
            country_df[
                "analytics_date"
            ]
            >= start_date
        )
        & (
            country_df[
                "analytics_date"
            ]
            <= end_date
        )
    ].copy()

    if selected_city != "All Cities":

        filtered_df = filtered_df[
            filtered_df["city_name"]
            == selected_city
        ].copy()

    if filtered_df.empty:

        st.warning(
            "No environmental observations "
            "match the selected filters."
        )

        st.stop()

    # Critical for Plotly time-series
    filtered_df = filtered_df.sort_values(
        "measured_at_utc"
    )

    # ===============================================================
    # SCOPE
    # ===============================================================

    if selected_city != "All Cities":

        scope_name = selected_city

    elif selected_country != "All Countries":

        scope_name = selected_country

    else:

        scope_name = "All Supported Locations"

    st.markdown(
        f"### {scope_name}"
    )

    st.caption(
        f"{start_date} → {end_date} · "
        f"{filtered_df['city_name'].nunique()} cities · "
        f"{len(filtered_df):,} city-hour observations"
    )

    # ===============================================================
    # LATEST SNAPSHOT
    # ===============================================================

    latest_timestamp = filtered_df[
        "measured_at_utc"
    ].max()

    latest_df = filtered_df[
        filtered_df["measured_at_utc"]
        == latest_timestamp
    ].copy()

    def latest_average(
        column: str,
    ) -> float:
        return latest_df[
            column
        ].mean()

    # ===============================================================
    # MAIN KPI CARDS
    # ===============================================================

    st.markdown(
        "#### Current Environmental Snapshot"
    )

    metric_1, metric_2, metric_3, metric_4, metric_5 = (
        st.columns(5)
    )

    metric_1.metric(
        "US AQI",
        f"{latest_average('us_aqi'):.0f}",
        delta=(
            f"Avg "
            f"{filtered_df['us_aqi'].mean():.0f}"
        ),
        delta_color="off",
    )

    metric_2.metric(
        "PM2.5",
        f"{latest_average('pm2_5'):.1f}",
        delta=(
            f"Avg "
            f"{filtered_df['pm2_5'].mean():.1f}"
        ),
        delta_color="off",
    )

    metric_3.metric(
        "PM10",
        f"{latest_average('pm10'):.1f}",
        delta=(
            f"Avg "
            f"{filtered_df['pm10'].mean():.1f}"
        ),
        delta_color="off",
    )

    metric_4.metric(
        "Temperature",
        f"{latest_average('temperature_2m'):.1f} °C",
        delta=(
            f"Avg "
            f"{filtered_df['temperature_2m'].mean():.1f} °C"
        ),
        delta_color="off",
    )

    metric_5.metric(
        "Humidity",
        f"{latest_average('relative_humidity_2m'):.0f}%",
        delta=(
            f"Avg "
            f"{filtered_df['relative_humidity_2m'].mean():.0f}%"
        ),
        delta_color="off",
    )

    st.caption(
        "Latest snapshot: "
        f"{latest_timestamp}"
    )

    st.divider()

    # ===============================================================
    # AIR QUALITY
    # ===============================================================

    st.header("Air Quality")

    st.caption(
        "Particulate matter, gaseous pollutants, "
        "AQI status and temporal behaviour."
    )

    # ---------------------------------------------------------------
    # PM2.5 / PM10 time-series
    # ---------------------------------------------------------------

    air_hourly = (
        filtered_df
        .groupby(
            "measured_at_utc",
            as_index=False,
        )
        .agg(
            PM2_5=(
                "pm2_5",
                "mean",
            ),
            PM10=(
                "pm10",
                "mean",
            ),
            US_AQI=(
                "us_aqi",
                "mean",
            ),
        )
        .sort_values(
            "measured_at_utc"
        )
    )

    st.markdown(
        "#### PM2.5 & PM10 Over Time"
    )

    pm_fig = px.line(
        air_hourly,
        x="measured_at_utc",
        y=[
            "PM2_5",
            "PM10",
        ],
        labels={
            "measured_at_utc":
                "Time",
            "value":
                "Concentration",
            "variable":
                "Metric",
        },
    )

    pm_fig = compact_figure(
        pm_fig,
        height=320,
    )

    st.plotly_chart(
        pm_fig,
        use_container_width=True,
    )

    # ---------------------------------------------------------------
    # AQI + status
    # ---------------------------------------------------------------

    air_left, air_right = st.columns(2)

    with air_left:

        st.markdown(
            "#### US AQI Over Time"
        )

        aqi_fig = px.line(
            air_hourly,
            x="measured_at_utc",
            y="US_AQI",
            labels={
                "measured_at_utc":
                    "Time",
                "US_AQI":
                    "US AQI",
            },
        )

        aqi_fig = compact_figure(
            aqi_fig,
            height=280,
            show_legend=False,
        )

        st.plotly_chart(
            aqi_fig,
            use_container_width=True,
        )

    with air_right:

        st.markdown(
            "#### AQ Status Distribution"
        )

        status_counts = (
            filtered_df[
                "air_quality_status"
            ]
            .fillna("unknown")
            .value_counts()
            .rename_axis(
                "Status"
            )
            .reset_index(
                name="Hours"
            )
        )

        status_fig = px.pie(
            status_counts,
            names="Status",
            values="Hours",
            hole=0.48,
        )

        status_fig = compact_figure(
            status_fig,
            height=280,
        )

        st.plotly_chart(
            status_fig,
            use_container_width=True,
        )

    # ---------------------------------------------------------------
    # Gaseous pollutant cards
    # ---------------------------------------------------------------

    st.markdown(
        "#### Latest Gaseous Pollutants"
    )

    gas_1, gas_2, gas_3, gas_4 = (
        st.columns(4)
    )

    gas_1.metric(
        "Carbon Monoxide",
        f"{latest_average('carbon_monoxide'):.1f}",
        delta=(
            f"Avg "
            f"{filtered_df['carbon_monoxide'].mean():.1f}"
        ),
        delta_color="off",
    )

    gas_2.metric(
        "Nitrogen Dioxide",
        f"{latest_average('nitrogen_dioxide'):.1f}",
        delta=(
            f"Avg "
            f"{filtered_df['nitrogen_dioxide'].mean():.1f}"
        ),
        delta_color="off",
    )

    gas_3.metric(
        "Sulphur Dioxide",
        f"{latest_average('sulphur_dioxide'):.1f}",
        delta=(
            f"Avg "
            f"{filtered_df['sulphur_dioxide'].mean():.1f}"
        ),
        delta_color="off",
    )

    gas_4.metric(
        "Ozone",
        f"{latest_average('ozone'):.1f}",
        delta=(
            f"Avg "
            f"{filtered_df['ozone'].mean():.1f}"
        ),
        delta_color="off",
    )

    # ---------------------------------------------------------------
    # City comparison only when useful
    # ---------------------------------------------------------------

    if (
        filtered_df[
            "city_name"
        ].nunique()
        > 1
    ):

        st.markdown(
            "#### PM2.5 Comparison by City"
        )

        city_pm25 = (
            filtered_df
            .groupby(
                "city_name",
                as_index=False,
            )
            .agg(
                Average_PM2_5=(
                    "pm2_5",
                    "mean",
                )
            )
            .sort_values(
                "Average_PM2_5",
                ascending=True,
            )
        )

        city_pm25_fig = px.bar(
            city_pm25,
            x="Average_PM2_5",
            y="city_name",
            orientation="h",
            labels={
                "Average_PM2_5":
                    "Average PM2.5",
                "city_name":
                    "City",
            },
        )

        city_pm25_fig = compact_figure(
            city_pm25_fig,
            height=300,
            show_legend=False,
        )

        st.plotly_chart(
            city_pm25_fig,
            use_container_width=True,
        )

    st.divider()

    # ===============================================================
    # WEATHER
    # ===============================================================

    st.header("Weather")

    weather_hourly = (
        filtered_df
        .groupby(
            "measured_at_utc",
            as_index=False,
        )
        .agg(
            Temperature=(
                "temperature_2m",
                "mean",
            ),
            Humidity=(
                "relative_humidity_2m",
                "mean",
            ),
            Precipitation=(
                "precipitation",
                "mean",
            ),
            Rain=(
                "rain",
                "mean",
            ),
            Pressure=(
                "surface_pressure",
                "mean",
            ),
            Cloud_Cover=(
                "cloud_cover",
                "mean",
            ),
            Wind_Speed=(
                "wind_speed_10m",
                "mean",
            ),
            Visibility=(
                "visibility",
                "mean",
            ),
        )
        .sort_values(
            "measured_at_utc"
        )
    )

    # ---------------------------------------------------------------
    # Weather secondary cards
    # ---------------------------------------------------------------

    weather_card_1, weather_card_2, weather_card_3, weather_card_4 = (
        st.columns(4)
    )

    weather_card_1.metric(
        "Precipitation",
        f"{latest_average('precipitation'):.2f}",
        delta=(
            f"Avg "
            f"{filtered_df['precipitation'].mean():.2f}"
        ),
        delta_color="off",
    )

    weather_card_2.metric(
        "Surface Pressure",
        f"{latest_average('surface_pressure'):.1f}",
        delta=(
            f"Avg "
            f"{filtered_df['surface_pressure'].mean():.1f}"
        ),
        delta_color="off",
    )

    weather_card_3.metric(
        "Cloud Cover",
        f"{latest_average('cloud_cover'):.0f}%",
        delta=(
            f"Avg "
            f"{filtered_df['cloud_cover'].mean():.0f}%"
        ),
        delta_color="off",
    )

    weather_card_4.metric(
        "Visibility",
        f"{latest_average('visibility'):.0f}",
        delta=(
            f"Avg "
            f"{filtered_df['visibility'].mean():.0f}"
        ),
        delta_color="off",
    )

    # ---------------------------------------------------------------
    # Temperature + humidity
    # ---------------------------------------------------------------

    weather_left, weather_right = (
        st.columns(2)
    )

    with weather_left:

        st.markdown(
            "#### Temperature"
        )

        temperature_fig = px.line(
            weather_hourly,
            x="measured_at_utc",
            y="Temperature",
            labels={
                "measured_at_utc":
                    "Time",
                "Temperature":
                    "Temperature",
            },
        )

        temperature_fig = compact_figure(
            temperature_fig,
            height=270,
            show_legend=False,
        )

        st.plotly_chart(
            temperature_fig,
            use_container_width=True,
        )

    with weather_right:

        st.markdown(
            "#### Relative Humidity"
        )

        humidity_fig = px.area(
            weather_hourly,
            x="measured_at_utc",
            y="Humidity",
            labels={
                "measured_at_utc":
                    "Time",
                "Humidity":
                    "Humidity (%)",
            },
        )

        humidity_fig = compact_figure(
            humidity_fig,
            height=270,
            show_legend=False,
        )

        st.plotly_chart(
            humidity_fig,
            use_container_width=True,
        )

    # ---------------------------------------------------------------
    # Rain + pressure
    # ---------------------------------------------------------------

    weather_left_2, weather_right_2 = (
        st.columns(2)
    )

    with weather_left_2:

        st.markdown(
            "#### Precipitation & Rain"
        )

        rain_fig = px.bar(
            weather_hourly,
            x="measured_at_utc",
            y=[
                "Precipitation",
                "Rain",
            ],
            barmode="group",
            labels={
                "measured_at_utc":
                    "Time",
                "value":
                    "Amount",
                "variable":
                    "Metric",
            },
        )

        rain_fig = compact_figure(
            rain_fig,
            height=270,
        )

        st.plotly_chart(
            rain_fig,
            use_container_width=True,
        )

    with weather_right_2:

        st.markdown(
            "#### Surface Pressure"
        )

        pressure_fig = px.line(
            weather_hourly,
            x="measured_at_utc",
            y="Pressure",
            labels={
                "measured_at_utc":
                    "Time",
                "Pressure":
                    "Surface Pressure",
            },
        )

        pressure_fig = compact_figure(
            pressure_fig,
            height=270,
            show_legend=False,
        )

        st.plotly_chart(
            pressure_fig,
            use_container_width=True,
        )

    # ---------------------------------------------------------------
    # Cloud + wind
    # ---------------------------------------------------------------

    weather_left_3, weather_right_3 = (
        st.columns(2)
    )

    with weather_left_3:

        st.markdown(
            "#### Cloud Cover"
        )

        cloud_fig = px.area(
            weather_hourly,
            x="measured_at_utc",
            y="Cloud_Cover",
            labels={
                "measured_at_utc":
                    "Time",
                "Cloud_Cover":
                    "Cloud Cover (%)",
            },
        )

        cloud_fig = compact_figure(
            cloud_fig,
            height=270,
            show_legend=False,
        )

        st.plotly_chart(
            cloud_fig,
            use_container_width=True,
        )

    with weather_right_3:

        st.markdown(
            "#### Wind Speed"
        )

        wind_fig = px.line(
            weather_hourly,
            x="measured_at_utc",
            y="Wind_Speed",
            labels={
                "measured_at_utc":
                    "Time",
                "Wind_Speed":
                    "Wind Speed",
            },
        )

        wind_fig = compact_figure(
            wind_fig,
            height=270,
            show_legend=False,
        )

        st.plotly_chart(
            wind_fig,
            use_container_width=True,
        )

    st.divider()

    # ===============================================================
    # DETAIL TABLE — hidden by default
    # ===============================================================

    with st.expander(
        "View hourly environmental observations"
    ):

        observation_columns = [
            "measured_at_local",
            "city_name",
            "country_name",

            "us_aqi",
            "air_quality_status",

            "pm2_5",
            "pm10",
            "carbon_monoxide",
            "nitrogen_dioxide",
            "sulphur_dioxide",
            "ozone",

            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "rain",

            "surface_pressure",
            "cloud_cover",
            "wind_speed_10m",
            "wind_direction_10m",
            "visibility",
            "weather_code",
        ]

        observations = (
            filtered_df[
                observation_columns
            ]
            .sort_values(
                "measured_at_local",
                ascending=False,
            )
            .head(500)
        )

        st.dataframe(
            observations,
            use_container_width=True,
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