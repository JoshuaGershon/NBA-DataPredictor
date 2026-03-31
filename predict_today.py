import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests


API_KEY = os.getenv("ODDS_API_KEY")
URL = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"
SNAPSHOT_FILE = "snapshots.csv"
CACHE_FILE = "odds_cache.json"
EASTERN_TZ = ZoneInfo("America/New_York")


def load_odds_cache():
    cache_path = Path(CACHE_FILE)
    if not cache_path.exists():
        return None

    try:
        with cache_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_odds_cache(events, fetch_time):
    payload = {
        "last_fetch_time": fetch_time.isoformat(),
        "events": events,
    }
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def parse_cache_time(value):
    if not value:
        return None
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()


def should_call_api(now_et, last_fetch_time):
    within_window = 12 <= now_et.hour < 22
    if not within_window:
        return False

    if last_fetch_time is None:
        return True

    last_fetch_et = last_fetch_time.astimezone(EASTERN_TZ)
    return not (
        last_fetch_et.year == now_et.year
        and last_fetch_et.month == now_et.month
        and last_fetch_et.day == now_et.day
        and last_fetch_et.hour == now_et.hour
    )


def get_events():
    now_et = pd.Timestamp.now(tz=EASTERN_TZ).to_pydatetime()
    cache = load_odds_cache() or {}
    cached_events = cache.get("events", [])
    last_fetch_time = parse_cache_time(cache.get("last_fetch_time"))

    print(f"Current ET time: {now_et.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")
    print(f"Last fetch time: {last_fetch_time.astimezone(EASTERN_TZ).strftime('%Y-%m-%d %I:%M:%S %p %Z') if last_fetch_time else 'None'}")

    if not should_call_api(now_et, last_fetch_time):
        print("USING CACHE")
        print(f"Number of events returned: {len(cached_events)}")
        return cached_events

    if not API_KEY:
        print("ODDS_API_KEY not set")
        print(f"Number of events returned: {len(cached_events)}")
        return cached_events

    try:
        response = requests.get(
            URL,
            params={
                "apiKey": API_KEY,
                "regions": "us",
                "markets": "h2h,spreads,totals",
                "oddsFormat": "american",
                "dateFormat": "iso",
            },
            timeout=30,
        )

        print(f"Status code: {response.status_code}")

        if response.status_code != 200:
            raise RuntimeError(f"Odds API request failed with status code {response.status_code}: {response.text}")

        events = response.json()
        save_odds_cache(events, pd.Timestamp.now(tz='UTC').to_pydatetime())
        print("API CALL MADE")
        print(f"Number of events returned: {len(events)}")
        return events
    except Exception:
        if cached_events:
            print("USING CACHE")
            print(f"Number of events returned: {len(cached_events)}")
            return cached_events
        raise


def load_snapshots():
    try:
        return pd.read_csv(SNAPSHOT_FILE)
    except FileNotFoundError:
        return pd.DataFrame()


def save_snapshots(df):
    if df.empty:
        return

    snapshot_df = df.copy()
    snapshot_df.insert(0, "timestamp", pd.Timestamp.utcnow().isoformat())
    snapshot_df.to_csv(SNAPSHOT_FILE, mode="a", header=not pd.io.common.file_exists(SNAPSHOT_FILE), index=False)


def get_preferred_bookmaker(event):
    bookmakers = event.get("bookmakers", [])
    if not bookmakers:
        return None

    for bookmaker in bookmakers:
        if bookmaker.get("key") == "fanduel" or bookmaker.get("title") == "FanDuel":
            return bookmaker

    return bookmakers[0]


def get_market(bookmaker, market_key):
    for market in bookmaker.get("markets", []):
        if market.get("key") == market_key:
            return market
    return None


def american_to_implied_probability(odds):
    odds = float(odds)
    if odds > 0:
        return 100 / (odds + 100)
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 0.5


def format_american_odds(odds):
    return f"{int(float(odds)):+}"


def format_point(point):
    value = float(point)
    if value > 0:
        return f"+{value:g}"
    return f"{value:g}"


def format_commence_time(value):
    dt = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(dt):
        return ""
    eastern = dt.tz_convert(EASTERN_TZ)
    return eastern.strftime("%b %d, %-I:%M %p ET")


def calculate_vig_free_probabilities(odds_a, odds_b):
    implied_a = american_to_implied_probability(odds_a)
    implied_b = american_to_implied_probability(odds_b)
    total = implied_a + implied_b

    if total == 0:
        return 0.5, 0.5, implied_a, implied_b

    vig_free_a = implied_a / total
    vig_free_b = implied_b / total
    return vig_free_a, vig_free_b, implied_a, implied_b


def clamp(value, low, high):
    return max(low, min(value, high))


def parse_line_value(market, selection, line_text):
    if not isinstance(line_text, str) or not line_text:
        return None

    try:
        left, right = line_text.split(" vs ")
    except ValueError:
        return None

    if market == "Moneyline":
        for side in (left, right):
            if side.startswith(f"{selection} "):
                return float(side.replace(f"{selection} ", ""))

    if market == "Spread":
        for side in (left, right):
            if side.startswith(f"{selection} "):
                rest = side.replace(f"{selection} ", "")
                point_text = rest.split(" ")[0]
                return float(point_text)

    if market == "Total":
        for side in (left, right):
            if side.startswith(f"{selection} "):
                return float(side.replace(f"{selection} ", "").split(" ")[0])

    return None


def parse_price_value(market, selection, line_text):
    if not isinstance(line_text, str) or not line_text:
        return None

    try:
        left, right = line_text.split(" vs ")
    except ValueError:
        return None

    if market == "Moneyline":
        for side in (left, right):
            if side.startswith(f"{selection} "):
                return float(side.replace(f"{selection} ", ""))

    if market == "Spread":
        for side in (left, right):
            if side.startswith(f"{selection} "):
                if "(" not in side or ")" not in side:
                    return None
                return float(side.split("(")[1].split(")")[0])

    if market == "Total":
        for side in (left, right):
            if side.startswith(f"{selection} "):
                if "(" not in side or ")" not in side:
                    return None
                return float(side.split("(")[1].split(")")[0])

    return None


def get_snapshot_history(snapshots_df, game, market, sportsbook):
    if snapshots_df.empty:
        return pd.DataFrame()

    history = snapshots_df[
        (snapshots_df["Game"] == game)
        & (snapshots_df["Market"] == market)
        & (snapshots_df["Sportsbook"] == sportsbook)
    ].copy()

    if history.empty:
        return history

    if "timestamp" in history.columns:
        history = history.sort_values("timestamp")

    return history


def get_open_and_current_line(history, current_line):
    if history.empty or "Sportsbook Line" not in history.columns:
        return None, current_line

    open_line = history.iloc[0]["Sportsbook Line"]
    latest_line = history.iloc[-1]["Sportsbook Line"]
    return open_line, latest_line or current_line


def get_open_line(history, fallback_line):
    if history.empty or "Sportsbook Line" not in history.columns:
        return fallback_line
    return history.iloc[0]["Sportsbook Line"]


def calculate_line_move(market, selection, open_line, current_line):
    open_value = parse_line_value(market, selection, open_line)
    current_value = parse_line_value(market, selection, current_line)

    if open_value is None or current_value is None:
        return None

    move = current_value - open_value
    return round(move, 1)


def calculate_price_move(market, selection, open_line, current_line):
    open_price = parse_price_value(market, selection, open_line)
    current_price = parse_price_value(market, selection, current_line)

    if open_price is None or current_price is None:
        return None

    return round(current_price - open_price, 1)


def calculate_timing_score(commence_time_text):
    cleaned_time = str(commence_time_text).replace(" ET", "").strip()
    dt = pd.to_datetime(cleaned_time, errors="coerce")
    if pd.isna(dt):
        return 50.0

    now_et = pd.Timestamp.now(tz=EASTERN_TZ)
    event_time = dt.tz_localize(EASTERN_TZ) if dt.tzinfo is None else dt.tz_convert(EASTERN_TZ)
    hours_to_game = max((event_time - now_et).total_seconds() / 3600, 0)
    return round(100 - min(hours_to_game, 12) / 12 * 100, 1)


def calculate_reverse_movement(market, selection, line_move, price_move, edge_value):
    if edge_value is None:
        return False

    if market == "Moneyline":
        if price_move is None:
            return False
        return edge_value > 0 and price_move > 0

    if market == "Spread":
        if line_move is None and price_move is None:
            return False
        return edge_value > 0 and ((line_move is not None and line_move < 0) or (price_move is not None and price_move > 0))

    if market == "Total":
        if line_move is None and price_move is None:
            return False
        if selection == "Over":
            return edge_value > 0 and ((line_move is not None and line_move > 0) or (price_move is not None and price_move > 0))
        if selection == "Under":
            return edge_value > 0 and ((line_move is not None and line_move < 0) or (price_move is not None and price_move > 0))

    return False


def calculate_signal_score(edge, line_move, price_move, reverse_movement, timing_score):
    edge_component = clamp((edge + 5) / 10 * 100, 0, 100) if edge is not None else 50
    movement_source = line_move if line_move is not None else price_move
    movement_component = clamp(abs(movement_source or 0) * 20, 0, 100)
    reverse_component = 100 if reverse_movement else 0
    timing_component = timing_score if timing_score is not None else 50

    score = (
        edge_component * 0.4
        + movement_component * 0.3
        + reverse_component * 0.2
        + timing_component * 0.1
    )
    return round(clamp(score, 0, 100), 1)


def normalize_signal_scores(df):
    if df.empty or "Signal Score" not in df.columns:
        return df

    data = df.copy()
    numeric_scores = pd.to_numeric(data["Signal Score"], errors="coerce")

    for market in data["Market"].dropna().unique():
        market_mask = data["Market"] == market
        market_scores = numeric_scores[market_mask]

        if market_scores.dropna().empty:
            continue

        min_score = market_scores.min()
        max_score = market_scores.max()

        if pd.isna(min_score) or pd.isna(max_score):
            continue

        if max_score == min_score:
            normalized = pd.Series([50.0] * market_mask.sum(), index=market_scores.index)
        else:
            normalized = 35 + ((market_scores - min_score) / (max_score - min_score)) * 65

        data.loc[market_mask, "Signal Score"] = normalized.round(1)

    return data


def calculate_confidence(model_probability):
    return round(clamp(50 + abs(model_probability - 0.5) * 90, 50, 95), 1)


def base_row(game, commence_time, market, sportsbook, sportsbook_line, recommended_pick, confidence, edge):
    return {
        "Game": game,
        "Commence Time": commence_time,
        "Market": market,
        "Sportsbook": sportsbook,
        "Sportsbook Line": sportsbook_line,
        "Recommended Pick": recommended_pick,
        "Confidence": f"{confidence:.1f}%",
        "Edge": f"{edge:.1f}%",
        "Open Line": None,
        "Current Line": sportsbook_line,
        "Line Move": None,
        "Price Move": None,
        "Reverse Movement": None,
        "Signal Score": None,
    }


def build_moneyline_row(game, commence_time, sportsbook, away_team, home_team, market, snapshots_df):
    prices = {}
    for outcome in market.get("outcomes", []):
        name = outcome.get("name")
        price = outcome.get("price")
        if name and price is not None:
            prices[name] = float(price)

    away_odds = prices.get(away_team)
    home_odds = prices.get(home_team)
    if away_odds is None or home_odds is None:
        return None

    away_vig_free, home_vig_free, away_implied, home_implied = calculate_vig_free_probabilities(away_odds, home_odds)
    sportsbook_line = f"{away_team} {format_american_odds(away_odds)} vs {home_team} {format_american_odds(home_odds)}"
    history = get_snapshot_history(snapshots_df, game, "Moneyline", sportsbook)
    open_line = get_open_line(history, sportsbook_line)
    home_open_odds = parse_price_value("Moneyline", home_team, open_line)
    away_open_odds = parse_price_value("Moneyline", away_team, open_line)
    home_price_move = 0 if home_open_odds is None else home_odds - home_open_odds
    away_price_move = 0 if away_open_odds is None else away_odds - away_open_odds

    price_gap = home_implied - away_implied
    home_model_probability = clamp(0.5 + 0.035 + (price_gap * 0.35) - (home_price_move / 400), 0.05, 0.95)
    away_model_probability = clamp(1 - home_model_probability, 0.05, 0.95)

    away_edge = (away_model_probability - away_implied) * 100
    home_edge = (home_model_probability - home_implied) * 100

    if away_edge >= home_edge:
        recommended_pick = away_team
        selected_model_probability = away_model_probability
        edge = away_edge
    else:
        recommended_pick = home_team
        selected_model_probability = home_model_probability
        edge = home_edge

    confidence = calculate_confidence(selected_model_probability)
    return base_row(game, commence_time, "Moneyline", sportsbook, sportsbook_line, recommended_pick, confidence, edge)


def build_spread_row(game, commence_time, sportsbook, away_team, home_team, market, snapshots_df):
    sides = {}
    for outcome in market.get("outcomes", []):
        name = outcome.get("name")
        price = outcome.get("price")
        point = outcome.get("point")
        if name and price is not None and point is not None:
            sides[name] = {"price": float(price), "point": float(point)}

    away_side = sides.get(away_team)
    home_side = sides.get(home_team)
    if away_side is None or home_side is None:
        return None

    away_vig_free, home_vig_free, away_implied, home_implied = calculate_vig_free_probabilities(
        away_side["price"], home_side["price"]
    )
    sportsbook_line = (
        f"{away_team} {format_point(away_side['point'])} ({format_american_odds(away_side['price'])}) vs "
        f"{home_team} {format_point(home_side['point'])} ({format_american_odds(home_side['price'])})"
    )
    history = get_snapshot_history(snapshots_df, game, "Spread", sportsbook)
    open_line = get_open_line(history, sportsbook_line)
    open_home_point = parse_line_value("Spread", home_team, open_line)
    current_home_margin = -home_side["point"]
    open_home_margin = current_home_margin if open_home_point is None else -open_home_point
    movement_adjustment = (current_home_margin - open_home_margin) * 0.75
    price_adjustment = (home_vig_free - away_vig_free) * 1.5
    projected_home_margin = current_home_margin + movement_adjustment + price_adjustment
    margin_gap = projected_home_margin - current_home_margin
    home_model_probability = clamp(0.5 + (margin_gap / 6.0) + (home_vig_free - 0.5) * 0.25, 0.05, 0.95)
    away_model_probability = clamp(1 - home_model_probability, 0.05, 0.95)
    away_edge = (away_model_probability - away_implied) * 100
    home_edge = (home_model_probability - home_implied) * 100

    if away_edge >= home_edge:
        recommended_pick = away_team
        selected_model_probability = away_model_probability
        selected_point = away_side["point"]
        edge = away_edge
    else:
        recommended_pick = home_team
        selected_model_probability = home_model_probability
        selected_point = home_side["point"]
        edge = home_edge

    confidence = calculate_confidence(selected_model_probability)
    edge = round(edge, 1)
    row = base_row(game, commence_time, "Spread", sportsbook, sportsbook_line, recommended_pick, confidence, edge)
    row["Recommended Pick"] = f"{recommended_pick} {format_point(selected_point)}"
    return row


def build_total_row(game, commence_time, sportsbook, market, snapshots_df):
    sides = {}
    for outcome in market.get("outcomes", []):
        name = outcome.get("name")
        price = outcome.get("price")
        point = outcome.get("point")
        if name and price is not None and point is not None:
            sides[name] = {"price": float(price), "point": float(point)}

    over_side = sides.get("Over")
    under_side = sides.get("Under")
    if over_side is None or under_side is None:
        return None

    over_vig_free, under_vig_free, over_implied, under_implied = calculate_vig_free_probabilities(
        over_side["price"], under_side["price"]
    )
    sportsbook_line = (
        f"Over {over_side['point']:g} ({format_american_odds(over_side['price'])}) vs "
        f"Under {under_side['point']:g} ({format_american_odds(under_side['price'])})"
    )
    history = get_snapshot_history(snapshots_df, game, "Total", sportsbook)
    open_line = get_open_line(history, sportsbook_line)
    current_total = over_side["point"]
    open_total = parse_line_value("Total", "Over", open_line)
    open_total = current_total if open_total is None else open_total
    movement_adjustment = (current_total - open_total) * 0.8
    price_adjustment = (over_vig_free - under_vig_free) * 3.0
    projected_total = current_total + movement_adjustment + price_adjustment
    total_gap = projected_total - current_total
    over_model_probability = clamp(0.5 + (total_gap / 8.0) + (over_vig_free - 0.5) * 0.2, 0.05, 0.95)
    under_model_probability = clamp(1 - over_model_probability, 0.05, 0.95)
    over_edge = (over_model_probability - over_implied) * 100
    under_edge = (under_model_probability - under_implied) * 100

    if over_edge > under_edge:
        recommended_pick = "Over"
        selected_model_probability = over_model_probability
        selected_point = over_side["point"]
        edge = over_edge
    elif under_edge > over_edge:
        recommended_pick = "Under"
        selected_model_probability = under_model_probability
        selected_point = under_side["point"]
        edge = under_edge
    elif over_vig_free >= under_vig_free:
        recommended_pick = "Over"
        selected_model_probability = over_model_probability
        selected_point = over_side["point"]
        edge = over_edge
    else:
        recommended_pick = "Under"
        selected_model_probability = under_model_probability
        selected_point = under_side["point"]
        edge = under_edge

    confidence = calculate_confidence(selected_model_probability)
    edge = round(edge, 1)
    row = base_row(game, commence_time, "Total", sportsbook, sportsbook_line, recommended_pick, confidence, edge)
    row["Recommended Pick"] = f"{recommended_pick} {selected_point:g}"
    return row


def enrich_signal_columns(row, snapshots_df):
    history = get_snapshot_history(snapshots_df, row["Game"], row["Market"], row["Sportsbook"])
    open_line, current_line = get_open_and_current_line(history, row["Current Line"])

    selection = row["Recommended Pick"]
    if row["Market"] == "Spread":
        selection = " ".join(str(selection).split(" ")[:-1])
    elif row["Market"] == "Total":
        selection = str(selection).split(" ")[0]

    line_move = calculate_line_move(row["Market"], selection, open_line, current_line)
    price_move = calculate_price_move(row["Market"], selection, open_line, current_line)
    edge_value = float(str(row["Edge"]).replace("%", "")) if row["Edge"] is not None else None
    reverse_movement = calculate_reverse_movement(row["Market"], selection, line_move, price_move, edge_value)
    timing_score = calculate_timing_score(row["Commence Time"])
    signal_score = calculate_signal_score(edge_value, line_move, price_move, reverse_movement, timing_score)

    row["Open Line"] = open_line
    row["Current Line"] = current_line
    row["Line Move"] = line_move
    row["Price Move"] = price_move
    row["Reverse Movement"] = reverse_movement
    row["Signal Score"] = signal_score
    return row


def build_predictions():
    events = get_events()
    snapshots_df = load_snapshots()
    rows = []
    snapshot_rows = []

    for event in events:
        away_team = event.get("away_team")
        home_team = event.get("home_team")

        if not away_team or not home_team:
            continue

        bookmaker = get_preferred_bookmaker(event)
        if not bookmaker:
            continue

        sportsbook = bookmaker.get("title", bookmaker.get("key", ""))
        game = f"{away_team} at {home_team}"
        commence_time = format_commence_time(event.get("commence_time"))
        moneyline_market = get_market(bookmaker, "h2h")

        if moneyline_market:
            row = build_moneyline_row(game, commence_time, sportsbook, away_team, home_team, moneyline_market, snapshots_df)
            if row:
                rows.append(enrich_signal_columns(row, snapshots_df))
                snapshot_rows.append(
                    {
                        "Game": row["Game"],
                        "Commence Time": row["Commence Time"],
                        "Market": row["Market"],
                        "Sportsbook": row["Sportsbook"],
                        "Sportsbook Line": row["Sportsbook Line"],
                        "Recommended Pick": row["Recommended Pick"],
                        "Confidence": row["Confidence"],
                        "Edge": row["Edge"],
                    }
                )

        spread_market = get_market(bookmaker, "spreads")
        if spread_market:
            row = build_spread_row(game, commence_time, sportsbook, away_team, home_team, spread_market, snapshots_df)
            if row:
                rows.append(enrich_signal_columns(row, snapshots_df))
                snapshot_rows.append(
                    {
                        "Game": row["Game"],
                        "Commence Time": row["Commence Time"],
                        "Market": row["Market"],
                        "Sportsbook": row["Sportsbook"],
                        "Sportsbook Line": row["Sportsbook Line"],
                        "Recommended Pick": row["Recommended Pick"],
                        "Confidence": row["Confidence"],
                        "Edge": row["Edge"],
                    }
                )

        total_market = get_market(bookmaker, "totals")
        if total_market:
            row = build_total_row(game, commence_time, sportsbook, total_market, snapshots_df)
            if row:
                rows.append(enrich_signal_columns(row, snapshots_df))
                snapshot_rows.append(
                    {
                        "Game": row["Game"],
                        "Commence Time": row["Commence Time"],
                        "Market": row["Market"],
                        "Sportsbook": row["Sportsbook"],
                        "Sportsbook Line": row["Sportsbook Line"],
                        "Recommended Pick": row["Recommended Pick"],
                        "Confidence": row["Confidence"],
                        "Edge": row["Edge"],
                    }
                )

    if snapshot_rows:
        save_snapshots(pd.DataFrame(snapshot_rows))

    result = pd.DataFrame(
        rows,
        columns=[
            "Game",
            "Commence Time",
            "Market",
            "Sportsbook",
            "Sportsbook Line",
            "Recommended Pick",
            "Confidence",
            "Edge",
            "Open Line",
            "Current Line",
            "Line Move",
            "Price Move",
            "Reverse Movement",
            "Signal Score",
        ],
    )
    return normalize_signal_scores(result)


def predict_today():
    return build_predictions()


if __name__ == "__main__":
    print(predict_today().to_string(index=False))
