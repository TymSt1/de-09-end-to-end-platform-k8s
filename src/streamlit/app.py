"""
NYC Taxi Data Platform Dashboard
Reads from dbt marts in PostgreSQL
"""

import os
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# Page config
st.set_page_config(
    page_title="NYC Taxi Platform",
    page_icon="🚕",
    layout="wide",
)


def get_connection():
    """Create a fresh database connection each time."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres.data.svc.cluster.local"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "warehouse"),
        user=os.getenv("DB_USER", "platform"),
        password=os.getenv("DB_PASSWORD", "platform_secret_2026"),
        connect_timeout=5,
    )


@st.cache_data(ttl=30)
def run_query(query):
    """Run a query with automatic reconnection."""
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()


# --- Header ---
st.title("🚕 NYC Taxi Data Platform")
st.markdown("Real-time analytics from the end-to-end data pipeline: **Kafka → S3 → Spark → dbt → PostgreSQL**")

# --- KPIs ---
rides = run_query(
    "SELECT COUNT(*) as total_rides, ROUND(AVG(total_amount)::numeric, 2) as avg_fare, ROUND(SUM(total_amount)::numeric, 2) as total_revenue, SUM(passenger_count) as total_passengers FROM public_marts.fact_rides"
)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Rides", f"{rides['total_rides'].iloc[0]:,}")
col2.metric("Avg Fare", f"${rides['avg_fare'].iloc[0]}")
col3.metric("Total Revenue", f"${rides['total_revenue'].iloc[0]:,.2f}")
col4.metric("Total Passengers", f"{rides['total_passengers'].iloc[0]:,}")

st.divider()

# --- Two column layout ---
left, right = st.columns(2)

# --- Zone Performance ---
with left:
    st.subheader("📍 Top Zones by Revenue")
    zones = run_query("""
        SELECT pickup_zone_name, total_trips, avg_fare,
               total_revenue, avg_tip_pct, revenue_rank
        FROM public_marts.dim_zones
        ORDER BY revenue_rank
        LIMIT 10
    """)

    fig_zones = px.bar(
        zones,
        x="total_revenue",
        y="pickup_zone_name",
        orientation="h",
        color="avg_tip_pct",
        color_continuous_scale="Greens",
        labels={
            "total_revenue": "Total Revenue ($)",
            "pickup_zone_name": "Zone",
            "avg_tip_pct": "Avg Tip %",
        },
    )
    fig_zones.update_layout(yaxis=dict(autorange="reversed"), height=400)
    st.plotly_chart(fig_zones, use_container_width=True)

# --- Payment Analysis ---
with right:
    st.subheader("💳 Payment Methods")
    payments = run_query("SELECT * FROM public_marts.dim_payment_analysis ORDER BY total_revenue DESC")

    fig_pay = px.pie(
        payments,
        values="total_revenue",
        names="payment_type",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_pay.update_layout(height=400)
    st.plotly_chart(fig_pay, use_container_width=True)

st.divider()

# --- Hourly Patterns ---
st.subheader("⏰ Hourly Trip Patterns")
hourly = run_query("SELECT * FROM public_marts.fact_hourly_summary ORDER BY event_date, event_hour")

fig_hourly = px.bar(
    hourly,
    x="event_hour",
    y="trip_count",
    color="avg_fare",
    color_continuous_scale="YlOrRd",
    labels={
        "event_hour": "Hour of Day",
        "trip_count": "Trip Count",
        "avg_fare": "Avg Fare ($)",
    },
)
fig_hourly.update_layout(height=350)
st.plotly_chart(fig_hourly, use_container_width=True)

st.divider()

# --- Trip Categories ---
left2, right2 = st.columns(2)

with left2:
    st.subheader("📏 Trip Distance Distribution")
    distances = run_query("""
        SELECT distance_category, COUNT(*) as count,
               ROUND(AVG(total_amount)::numeric, 2) as avg_fare
        FROM public_marts.fact_rides
        GROUP BY distance_category
        ORDER BY CASE distance_category
            WHEN 'short' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END
    """)

    fig_dist = px.bar(
        distances,
        x="distance_category",
        y="count",
        color="avg_fare",
        color_continuous_scale="Blues",
        labels={
            "distance_category": "Distance",
            "count": "Trips",
            "avg_fare": "Avg Fare ($)",
        },
    )
    fig_dist.update_layout(height=350)
    st.plotly_chart(fig_dist, use_container_width=True)

with right2:
    st.subheader("💰 Fare Categories")
    fares = run_query("""
        SELECT fare_category, COUNT(*) as count,
               ROUND(AVG(tip_percentage)::numeric, 2) as avg_tip_pct
        FROM public_marts.fact_rides
        GROUP BY fare_category
        ORDER BY CASE fare_category
            WHEN 'budget' THEN 1 WHEN 'standard' THEN 2 ELSE 3 END
    """)

    fig_fare = px.bar(
        fares,
        x="fare_category",
        y="count",
        color="avg_tip_pct",
        color_continuous_scale="Purples",
        labels={
            "fare_category": "Fare Category",
            "count": "Trips",
            "avg_tip_pct": "Avg Tip %",
        },
    )
    fig_fare.update_layout(height=350)
    st.plotly_chart(fig_fare, use_container_width=True)

st.divider()

# --- Borough Flow ---
st.subheader("🗺️ Borough-to-Borough Flow")
flows = run_query("""
    SELECT borough_flow, COUNT(*) as trips,
           ROUND(AVG(total_amount)::numeric, 2) as avg_fare
    FROM public_marts.fact_rides
    GROUP BY borough_flow
    ORDER BY trips DESC
    LIMIT 10
""")

fig_flow = px.bar(
    flows,
    x="trips",
    y="borough_flow",
    orientation="h",
    color="avg_fare",
    color_continuous_scale="Viridis",
    labels={
        "trips": "Trip Count",
        "borough_flow": "Route",
        "avg_fare": "Avg Fare ($)",
    },
)
fig_flow.update_layout(yaxis=dict(autorange="reversed"), height=400)
st.plotly_chart(fig_flow, use_container_width=True)

# --- Footer ---
st.divider()
st.caption(
    "Data pipeline: Kafka (streaming) → S3/LocalStack (storage) → Spark (processing) → dbt (transformation) → PostgreSQL (warehouse) | Orchestrated by Airflow on Kubernetes"
)
