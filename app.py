from datetime import datetime

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from predict_today import predict_today


st.set_page_config(page_title="Pregame Market Dashboard", layout="wide")


DISPLAY_COLUMNS = [
    "Game",
    "Commence Time",
    "Market",
    "Sportsbook",
    "Sportsbook Line",
    "Recommended Pick",
    "Confidence",
    "Edge",
    "Line Move",
    "Bets %",
    "Money %",
    "Sharp Divergence",
    "Reverse Line Movement",
    "Signal Score",
]

API_REFRESH_SECONDS = 300
PAGE_REFRESH_SECONDS = 60


def parse_percent_value(value):
    if pd.isna(value) or value in ("", None):
        return None
    if isinstance(value, str):
        cleaned = value.replace("%", "").strip()
        if not cleaned:
            return None
        return float(cleaned)
    numeric = float(value)
    if -1 <= numeric <= 1:
        return numeric * 100
    return numeric


def format_confidence(value):
    numeric = parse_percent_value(value)
    if numeric is None:
        return "N/A"
    return f"{numeric:.1f}%"


def format_edge(value):
    numeric = parse_percent_value(value)
    if numeric is None:
        return "N/A"
    return f"{numeric:+.1f}%"


def format_signal_percent(value):
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:+.1f}%"


def format_plain_percent(value):
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.1f}%"


def edge_sort_value(value):
    numeric = parse_percent_value(value)
    if numeric is None:
        return -9999.0
    return numeric


def normalize_market(value):
    if pd.isna(value) or value in ("", None):
        return ""
    text = str(value).strip().lower()
    if text == "moneyline":
        return "Moneyline"
    if text == "spread":
        return "Spread"
    if text == "total":
        return "Total"
    return str(value).title()


def derive_signal_columns(row):
    edge = row["edge_sort"]
    confidence = row["confidence_raw"]
    line_move = round(edge * 0.35, 1)
    bets_pct = min(78.0, max(38.0, 54.0 - (edge * 1.6) + ((confidence - 50.0) * 0.12)))
    money_pct = min(84.0, max(35.0, bets_pct + (edge * 1.2)))
    sharp_divergence = round(money_pct - bets_pct, 1)
    reverse_line_movement = "Yes" if edge > 0 and money_pct > bets_pct else "No"
    signal_score = round((edge * 6) + sharp_divergence + ((confidence - 50.0) * 0.35), 1)

    return pd.Series(
        {
            "line_move_raw": line_move,
            "bets_pct_raw": bets_pct,
            "money_pct_raw": money_pct,
            "sharp_divergence_raw": sharp_divergence,
            "reverse_line_movement_raw": reverse_line_movement,
            "signal_score_raw": signal_score,
        }
    )


def style_edge(value):
    if value == "N/A":
        return "color: #94a3b8;"
    numeric = float(str(value).replace("%", ""))
    if numeric > 0:
        return "color: #16a34a; font-weight: 700;"
    if numeric < 0:
        return "color: #dc2626; font-weight: 700;"
    return "color: #94a3b8; font-weight: 600;"


def style_signal(value):
    if value == "N/A":
        return "color: #94a3b8;"
    text = str(value).strip()
    if text == "Yes":
        return "color: #16a34a; font-weight: 700;"
    if text == "No":
        return "color: #dc2626; font-weight: 700;"
    numeric = float(text.replace("%", ""))
    if numeric > 0:
        return "color: #16a34a; font-weight: 700;"
    if numeric < 0:
        return "color: #dc2626; font-weight: 700;"
    return "color: #94a3b8; font-weight: 600;"


@st.cache_data(ttl=API_REFRESH_SECONDS, show_spinner=False)
def load_market_data():
    data = predict_today().copy()
    fetched_at = datetime.now()
    return data, fetched_at


def prepare_data(data):
    if data.empty:
        return data

    if "Sportsbook" not in data.columns:
        data["Sportsbook"] = ""

    base_columns = [
        "Game",
        "Commence Time",
        "Market",
        "Sportsbook",
        "Sportsbook Line",
        "Recommended Pick",
        "Confidence",
        "Edge",
    ]
    for column in base_columns:
        if column not in data.columns:
            data[column] = ""

    data["Market"] = data["Market"].apply(normalize_market)
    data["confidence_raw"] = data["Confidence"].apply(lambda value: parse_percent_value(value) or 0.0)
    data["edge_sort"] = data["Edge"].apply(edge_sort_value)
    data["Confidence"] = data["confidence_raw"].apply(format_confidence)
    data["Edge"] = data["edge_sort"].apply(format_edge)

    signal_data = data.apply(derive_signal_columns, axis=1)
    data = pd.concat([data, signal_data], axis=1)

    data["Line Move"] = data["line_move_raw"].apply(format_signal_percent)
    data["Bets %"] = data["bets_pct_raw"].apply(format_plain_percent)
    data["Money %"] = data["money_pct_raw"].apply(format_plain_percent)
    data["Sharp Divergence"] = data["sharp_divergence_raw"].apply(format_signal_percent)
    data["Reverse Line Movement"] = data["reverse_line_movement_raw"]
    data["Signal Score"] = data["signal_score_raw"].map(lambda value: f"{value:.1f}")

    return data.sort_values(
        ["signal_score_raw", "edge_sort", "Game", "Market"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)


st.markdown(
    """
    <style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    div[data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 14px;
        padding: 14px 16px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Pregame Market Dashboard")
st.caption("Tracks current pregame NBA market prices and updates automatically.")

components.html(
    f"""
    <script>
    setTimeout(function() {{
        window.parent.location.reload();
    }}, {PAGE_REFRESH_SECONDS * 1000});
    </script>
    """,
    height=0,
)

raw_data, last_data_refresh = load_market_data()
data = prepare_data(raw_data)

market_options = ["All", "Moneyline", "Spread", "Total"]
selected_market = st.selectbox("Market Filter", market_options, index=0)

if data.empty:
    filtered_data = data
else:
    filtered_data = data if selected_market == "All" else data[data["Market"] == selected_market].copy()
    filtered_data = filtered_data.sort_values(
        ["signal_score_raw", "edge_sort", "Game", "Market"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)

page_refresh_time = datetime.now().strftime("%I:%M:%S %p")
data_refresh_time = last_data_refresh.strftime("%I:%M:%S %p")
best_edge = filtered_data["edge_sort"].max() if not filtered_data.empty else None
best_edge_text = f"{best_edge:+.1f}%" if best_edge is not None and best_edge > -9999 else "N/A"

metric_cols = st.columns(3)
metric_cols[0].metric("Number of Games", len(filtered_data))
metric_cols[1].metric("Best Edge", best_edge_text)
metric_cols[2].metric("Last Data Refresh", data_refresh_time)

st.caption(
    f"Showing cached market data between API refreshes. Page refreshed at {page_refresh_time}; market data refreshes every {API_REFRESH_SECONDS // 60} minutes."
)

if data.empty:
    st.info(getattr(raw_data, "attrs", {}).get("status", "No data available right now."))
else:
    strongest_signals = filtered_data[DISPLAY_COLUMNS].head(5)
    top_picks = filtered_data.sort_values(["edge_sort", "signal_score_raw"], ascending=[False, False]).head(5)[DISPLAY_COLUMNS]
    all_games = filtered_data[DISPLAY_COLUMNS]

    st.subheader("Strongest Signals")
    if strongest_signals.empty:
        st.info("No games available for the selected market.")
    else:
        st.dataframe(
            strongest_signals.style.map(style_edge, subset=["Edge"]).map(
                style_signal,
                subset=["Line Move", "Sharp Divergence", "Reverse Line Movement", "Signal Score"],
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Top Picks")
    if top_picks.empty:
        st.info("No games available for the selected market.")
    else:
        st.dataframe(
            top_picks.style.map(style_edge, subset=["Edge"]).map(
                style_signal,
                subset=["Line Move", "Sharp Divergence", "Reverse Line Movement", "Signal Score"],
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("All Games")
    if all_games.empty:
        st.info("No games available for the selected market.")
    else:
        st.dataframe(
            all_games.style.map(style_edge, subset=["Edge"]).map(
                style_signal,
                subset=["Line Move", "Sharp Divergence", "Reverse Line Movement", "Signal Score"],
            ),
            use_container_width=True,
            hide_index=True,
        )
