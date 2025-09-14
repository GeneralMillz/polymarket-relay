import streamlit as st
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# Database connection
DB_URL = "postgresql+psycopg2://jerry:kiefer@localhost:5432/prediction_markets"
engine = create_engine(DB_URL, future=True)

# ─────────────────────────────────────────────
# Helpers
def get_last_updated(full_table):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(f"SELECT MAX(ts_utc) AS last_updated FROM {full_table}"), conn)
        if not df.empty and pd.notnull(df.iloc[0, 0]):
            return pd.to_datetime(df.iloc[0, 0]).strftime("%Y-%m-%d %H:%M:%S")
        return "No data"
    except Exception as e:
        return f"Error: {e}"

def load_data(full_table, limit=100):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(f"SELECT * FROM {full_table} ORDER BY 1 DESC LIMIT {limit}"), conn)
        return df
    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})

def load_token_ids(table, column, limit=100):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(f"""
                SELECT DISTINCT {column}
                FROM {table}
                WHERE {column} IS NOT NULL
                ORDER BY {column} DESC
                LIMIT :limit
            """), conn, params={"limit": limit})
        return df[column].astype(str).tolist()
    except Exception:
        return []

def load_feed_data(table, filter_column, filter_value, columns, order_by="ts_utc"):
    try:
        with engine.connect() as conn:
            query = f"""
                SELECT {', '.join(columns)}
                FROM {table}
                WHERE {filter_column} = :value
                ORDER BY {order_by} ASC
            """
            df = pd.read_sql(text(query), conn, params={"value": filter_value})
        return df
    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})

def load_events_data(limit=100):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text("""
                SELECT *
                FROM hourly_polymarket.events
                WHERE ts_utc IS NOT NULL
                ORDER BY ts_utc DESC
                LIMIT :limit
            """), conn, params={"limit": limit})
        return df
    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})

def load_schema_status():
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM dashboard.schema_status"), conn)
        if df.empty:
            return None
        row = df.iloc[0]
        last_migration = pd.to_datetime(row["last_migration"])
        delta_days = (datetime.now() - last_migration).days

        if delta_days > 7:
            freshness = "🔴 Stale"
        elif delta_days > 3:
            freshness = "🟡 Aging"
        else:
            freshness = "🟢 Fresh"

        return {
            "total_markets": row["total_markets"],
            "orderbook_enabled": row["orderbook_enabled"],
            "active_markets": row["active_markets"],
            "last_migration": last_migration.strftime("%Y-%m-%d %H:%M:%S"),
            "freshness": freshness
        }
    except Exception as e:
        return {"Error": str(e)}

# ─────────────────────────────────────────────
# Streamlit UI
st.set_page_config(page_title="Meatball Multi Dashboard", layout="wide")
st.title("📊 Meatball Multi Dashboard")

tabs = st.tabs([
    "Kalshi", "Manifold", "Polymarket Markets", "Signals", "Schema",
    "Candles", "Orderbook", "Trades", "Events", "Notes"
])

with tabs[0]:
    st.header("Kalshi Markets")
    st.markdown(f"**Last Updated:** {get_last_updated('hourly_kalshi.markets')}")
    df = load_data("hourly_kalshi.markets")
    if not df.empty:
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("No Kalshi data available.")

with tabs[1]:
    st.header("Manifold Markets")
    st.markdown(f"**Last Updated:** {get_last_updated('hourly_manifold.markets')}")
    df = load_data("hourly_manifold.markets")
    if not df.empty:
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("No Manifold data available.")

with tabs[2]:
    st.header("Polymarket Markets")
    st.markdown(f"**Last Updated:** {get_last_updated('hourly_polymarket.markets')}")
    df = load_data("hourly_polymarket.markets")
    if not df.empty:
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("No Polymarket data available.")

with tabs[3]:
    st.header("📡 Market Signals")
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM public.market_signals"), conn)

        if df.empty:
            st.warning("No signals available.")
        else:
            # Format columns safely
            df["momentum_score"] = df["momentum_score"].apply(
                lambda s: f"🟢 {s:.2f}" if s is not None and s >= 0.7
                else (f"🟡 {s:.2f}" if s is not None and s >= 0.4
                else (f"🔴 {s:.2f}" if s is not None else "⚪ N/A"))
            )
            df["priority_score"] = df["priority_score"].apply(lambda p: f"{p:.2f}" if p is not None else "N/A")
            df["current_prob"] = df["current_prob"].apply(lambda p: f"{p:.1%}" if p is not None else "N/A")
            df["prob_change_6h"] = df["prob_change_6h"].apply(lambda p: f"{p:+.1%}" if p is not None else "N/A")
            df["volume_spike_ratio"] = df["volume_spike_ratio"].apply(lambda v: f"{v:.2f}" if v is not None else "N/A")
            df["liquidity_score"] = df["liquidity_score"].apply(lambda l: f"{l:.2f}" if l is not None else "N/A")
            df["spread"] = df["spread"].apply(lambda s: f"{s:.2f}" if s is not None else "N/A")
            df["cross_market_gap"] = df["cross_market_gap"].apply(lambda g: f"{g:.2f}" if g is not None else "N/A")

            st.dataframe(
                df[[
                    "ticker", "title", "current_prob", "prob_change_6h",
                    "momentum_score", "cross_market_gap", "spread",
                    "liquidity_score", "volume_spike_ratio", "priority_score"
                ]],
                use_container_width=True
            )
    except Exception as e:
        st.error(f"Failed to load signals: {e}")
with tabs[4]:
    st.header("Schema Status")
    status = load_schema_status()
    if status and "Error" not in status:
        st.markdown(f"""
        - **Total Markets:** `{status['total_markets']}`
        - **Orderbook Enabled:** `{status['orderbook_enabled']}`
        - **Active Markets:** `{status['active_markets']}`
        - **Last Migration:** `{status['last_migration']}`  
        - **Freshness:** {status['freshness']}
        """)
    else:
        st.warning(f"Schema status unavailable: {status.get('Error', 'Unknown error')}")

with tabs[5]:
    st.header("Polymarket Candles")
    token_list = load_token_ids("hourly_polymarket.candles", "yes_token_id")
    selected_token = st.selectbox("Select Token ID", token_list, key="candles_token")
    if selected_token:
        df = load_feed_data("hourly_polymarket.candles", "yes_token_id", selected_token,
                            ["ts_utc", "open", "high", "low", "close", "volume"])
        if not df.empty and "Error" not in df.columns:
            st.line_chart(df.set_index("ts_utc")[["open", "close"]])
            st.bar_chart(df.set_index("ts_utc")["volume"])
        else:
            st.warning("No candle data available for this token.")

with tabs[6]:
    st.header("Polymarket Orderbook")
    token_list = load_token_ids("hourly_polymarket.orderbook", "yes_token_id")
    selected_token = st.selectbox("Select Token ID", token_list, key="orderbook_token")
    if selected_token:
        df = load_feed_data("hourly_polymarket.orderbook", "yes_token_id", selected_token,
                            ["ts_utc", "bid_price", "ask_price", "bid_size", "ask_size"])
        if not df.empty and "Error" not in df.columns:
            df["spread"] = df["ask_price"] - df["bid_price"]
            st.subheader("Bid / Ask Spread")
            st.line_chart(df.set_index("ts_utc")[["bid_price", "ask_price", "spread"]])
            st.subheader("Liquidity")
            df["liquidity"] = df["bid_size"] + df["ask_size"]
            st.bar_chart(df.set_index("ts_utc")["liquidity"])
        else:
            st.warning("No orderbook data available for this token.")

with tabs[7]:
    st.header("Polymarket Trades")
    market_list = load_token_ids("hourly_polymarket.trades", "market_id")
    st.markdown(f"**Last Updated:** {get_last_updated('hourly_polymarket.trades')}")
    selected_market = st.selectbox("Select Market ID", market_list, key="trades_market")
    if selected_market:
        df = load_feed_data("hourly_polymarket.trades", "market_id", selected_market,
                            ["ts_utc", "price", "size", "side", "trade_id"])
        if not df.empty and "Error" not in df.columns:
            st.subheader("Trade Prices")
            st.line_chart(df.set_index("ts_utc")["price"])
            st.subheader("Trade Volume")
            st.bar_chart(df.set_index("ts_utc")["size"])
        else:
            st.warning("No trade data available for this market.")

with tabs[8]:
    st.header("Polymarket Events")
    df = load_events_data()
    st.markdown(f"**Last Updated:** {get_last_updated('hourly_polymarket.events')}")

    if not df.empty and "Error" not in df.columns:
        st.subheader("Event Table")
        st.dataframe(
            df[[
                "event_id", "name", "title", "category",
                "start_date", "end_date", "status", "markets", "ts_utc"
            ]],
            use_container_width=True
        )
        st.subheader("Raw Preview")
        st.write(df.head())
    else:
        st.warning("No event data available.")
with tabs[9]:
    st.header("🗒️ Project Notes & To-Do List")

    st.markdown("""
    ### 🔧 Next Tasks
    - **Fix Resolved Fetcher**: Ensure resolved markets are ingested and status flags are accurate.
    - **Candles Tab**: Patch missing token IDs and verify recent data ingestion.
    - **Orderbook Tab**: Confirm bid/ask updates and clean up inactive tokens.
    - **Event Data Refresh**: Explore Polymarket `/markets` endpoint for fresher event metadata.

    ---
    _Last updated: Sept 13, 2025_
    """)
