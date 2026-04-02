import json
import os
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests


API_KEY = os.getenv("ODDS_API_KEY")
ODDS_URL = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"
CACHE_FILE = Path("odds_cache.json")
SNAPSHOT_FILE = Path("snapshots.csv")
EASTERN_TZ = ZoneInfo("America/New_York")
ALLOWED_START_HOUR_ET = 12
ALLOWED_END_HOUR_ET = 22

OUTPUT_COLUMNS = [
    "Game ID",
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
]


def parse_commence_time(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def format_commence_time(commence_time_utc):
    if commence_time_utc is None:
        return ""
    return commence_time_utc.astimezone(EASTERN_TZ).strftime("%b %d, %-I:%M %p ET")


def clamp(value, low, high):
    return max(low, min(value, high))


def american_to_implied_probability(odds):
    odds = float(odds)
    if odds > 0:
        return 100 / (odds + 100)
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 0.5


def vig_free_probabilities(price_a, price_b):
    implied_a = american_to_implied_probability(price_a)
    implied_b = american_to_implied_probability(price_b)
    total = implied_a + implied_b

    if total == 0:
        return 0.5, 0.5, implied_a, implied_b

    return implied_a / total, implied_b / total, implied_a, implied_b


def format_american_odds(price):
    return f"{int(float(price)):+}"


def format_point(value):
    value = float(value)
    if value > 0:
        return f"+{value:g}"
    return f"{value:g}"


def build_game_id(away_team, home_team, commence_time_utc):
    ts = commence_time_utc.strftime("%Y%m%dT%H%M%SZ") if commence_time_utc else "unknown"
    return f"{away_team}_{home_team}_{ts}"


def load_cache():
    if not CACHE_FILE.exists():
        return None

    try:
        with CACHE_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_cache(events):
    payload = {
        "last_fetch_time": datetime.now(timezone.utc).isoformat(),
        "source": "API",
        "events": events,
    }
    with CACHE_FILE.open("w", encoding="utf-8") as f:
        json.dump(payload, f)


def count_future_events(events):
    now_utc = datetime.now(timezone.utc)
    count = 0
    for event in events or []:
        commence_time = parse_commence_time(event.get("commence_time"))
        if commence_time and commence_time > now_utc:
            count += 1
    return count


def within_api_window(now_et):
    return ALLOWED_START_HOUR_ET <= now_et.hour < ALLOWED_END_HOUR_ET


def already_fetched_this_hour(last_fetch_time, now_et):
    if last_fetch_time is None:
        return False

    last_fetch_et = last_fetch_time.astimezone(EASTERN_TZ)
    return (
        last_fetch_et.year == now_et.year
        and last_fetch_et.month == now_et.month
        and last_fetch_et.day == now_et.day
        and last_fetch_et.hour == now_et.hour
    )


def fetch_from_api():
    response = requests.get(
        ODDS_URL,
        params={
            "apiKey": API_KEY,
            "regions": "us",
            "markets": "h2h,spreads,totals",
            "oddsFormat": "american",
            "dateFormat": "iso",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json(), response.text


def get_live_odds():
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(EASTERN_TZ)
    cache = load_cache() or {}
    cached_events = cache.get("events", [])
    last_fetch_raw = cache.get("last_fetch_time")
    last_fetch_time = parse_commence_time(last_fetch_raw) if last_fetch_raw else None
    cached_future_games = count_future_events(cached_events)

    print(f"Current UTC time: {now_utc.isoformat()}")
    print(f"Current ET time: {now_et.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")
    print(f"Last fetch time: {last_fetch_time.isoformat() if last_fetch_time else 'None'}")
    print(f"Number of cached future games: {cached_future_games}")

    stale_cache = bool(cached_events) and cached_future_games == 0
    if stale_cache:
        print("Stale cache was rejected because all games were in the past")

    should_call = False
    if API_KEY:
        if not cached_events:
            should_call = True
        elif stale_cache:
            should_call = True
        elif within_api_window(now_et) and not already_fetched_this_hour(last_fetch_time, now_et):
            should_call = True

    if should_call:
        try:
            events, raw_text = fetch_from_api()
            print("Source: API")
            print(f"Raw response length: {len(raw_text)}")
            print(f"Number of raw API events: {len(events)}")
            print(f"First 3 commence_time values: {[event.get('commence_time') for event in events[:3]]}")

            if len(events) == 0:
                print("API returned 0 events")

            save_cache(events)
            return events
        except Exception as exc:
            print(f"API fetch failed: {exc}")
            if cached_future_games > 0:
                print("Source: CACHE")
                return cached_events
            return []

    if cached_events:
        print("Source: CACHE")
        print(f"Number of raw API events: {len(cached_events)}")
        print(f"First 3 commence_time values: {[event.get('commence_time') for event in cached_events[:3]]}")
        return cached_events

    print("API returned 0 events")
    return []


def filter_future_events(events):
    now_utc = datetime.now(timezone.utc)
    future_events = []

    print(f"Number of raw API events: {len(events)}")
    print(f"First 3 commence_time values: {[event.get('commence_time') for event in events[:3]]}")

    for event in events:
        commence_time = parse_commence_time(event.get("commence_time"))
        if commence_time and commence_time > now_utc:
            future_events.append(event)

    print(f"Number of future events after filtering: {len(future_events)}")

    if events and not future_events:
        print("Raw API had events, but all were filtered out as past games")

    return future_events


def load_snapshots():
    if not SNAPSHOT_FILE.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(SNAPSHOT_FILE)
    except Exception:
        return pd.DataFrame()


def save_snapshots(rows_df):
    if rows_df.empty:
        return

    snapshot_df = rows_df.copy()
    snapshot_df.insert(0, "timestamp", datetime.now(timezone.utc).isoformat())
    snapshot_df.to_csv(
        SNAPSHOT_FILE,
        mode="a",
        header=not SNAPSHOT_FILE.exists(),
        index=False,
    )


def get_preferred_bookmaker(event):
    bookmakers = event.get("bookmakers", [])
    if not bookmakers:
        return None

    for bookmaker in bookmakers:
        if bookmaker.get("key") == "fanduel" or bookmaker.get("title") == "FanDuel":
            return bookmaker

    return bookmakers[0]


def get_market(bookmaker, key):
    for market in bookmaker.get("markets", []):
        if market.get("key") == key:
            return market
    return None


def get_snapshot_history(snapshots_df, game_id, market, sportsbook):
    if snapshots_df.empty:
        return pd.DataFrame()

    history = snapshots_df[
        (snapshots_df["Game ID"] == game_id)
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
        return current_line, current_line

    open_line = history.iloc[0]["Sportsbook Line"]
    current_from_history = history.iloc[-1]["Sportsbook Line"]
    return open_line, current_line or current_from_history


def parse_moneyline_prices(line_text):
    try:
        left, right = line_text.split(" vs ")
        away_team = " ".join(left.split(" ")[:-1])
        away_price = float(left.split(" ")[-1])
        home_team = " ".join(right.split(" ")[:-1])
        home_price = float(right.split(" ")[-1])
        return {
            away_team: away_price,
            home_team: home_price,
        }
    except Exception:
        return {}


def parse_spread_sides(line_text):
    try:
        left, right = line_text.split(" vs ")
        left_name = left.split(" (")[0]
        right_name = right.split(" (")[0]
        left_team = " ".join(left_name.split(" ")[:-1])
        right_team = " ".join(right_name.split(" ")[:-1])
        left_point = float(left_name.split(" ")[-1])
        right_point = float(right_name.split(" ")[-1])
        left_price = float(left.split("(")[1].split(")")[0])
        right_price = float(right.split("(")[1].split(")")[0])
        return {
            left_team: {"point": left_point, "price": left_price},
            right_team: {"point": right_point, "price": right_price},
        }
    except Exception:
        return {}


def parse_total_sides(line_text):
    try:
        left, right = line_text.split(" vs ")
        left_name = left.split(" (")[0]
        right_name = right.split(" (")[0]
        left_side = left_name.split(" ")[0]
        right_side = right_name.split(" ")[0]
        left_point = float(left_name.split(" ")[1])
        right_point = float(right_name.split(" ")[1])
        left_price = float(left.split("(")[1].split(")")[0])
        right_price = float(right.split("(")[1].split(")")[0])
        return {
            left_side: {"point": left_point, "price": left_price},
            right_side: {"point": right_point, "price": right_price},
        }
    except Exception:
        return {}


def calculate_line_move(market, recommended_pick, open_line, current_line):
    if not open_line or not current_line:
        return None

    if market == "Moneyline":
        open_prices = parse_moneyline_prices(open_line)
        current_prices = parse_moneyline_prices(current_line)
        if recommended_pick not in open_prices or recommended_pick not in current_prices:
            return None
        return round(current_prices[recommended_pick] - open_prices[recommended_pick], 1)

    if market == "Spread":
        team = " ".join(recommended_pick.split(" ")[:-1])
        open_sides = parse_spread_sides(open_line)
        current_sides = parse_spread_sides(current_line)
        if team not in open_sides or team not in current_sides:
            return None
        return round(current_sides[team]["point"] - open_sides[team]["point"], 1)

    if market == "Total":
        side = recommended_pick.split(" ")[0]
        open_sides = parse_total_sides(open_line)
        current_sides = parse_total_sides(current_line)
        if side not in open_sides or side not in current_sides:
            return None
        return round(current_sides[side]["point"] - open_sides[side]["point"], 1)

    return None


def calculate_price_move(market, recommended_pick, open_line, current_line):
    if not open_line or not current_line:
        return None

    if market == "Moneyline":
        open_prices = parse_moneyline_prices(open_line)
        current_prices = parse_moneyline_prices(current_line)
        if recommended_pick not in open_prices or recommended_pick not in current_prices:
            return None
        return round(current_prices[recommended_pick] - open_prices[recommended_pick], 1)

    if market == "Spread":
        team = " ".join(recommended_pick.split(" ")[:-1])
        open_sides = parse_spread_sides(open_line)
        current_sides = parse_spread_sides(current_line)
        if team not in open_sides or team not in current_sides:
            return None
        return round(current_sides[team]["price"] - open_sides[team]["price"], 1)

    if market == "Total":
        side = recommended_pick.split(" ")[0]
        open_sides = parse_total_sides(open_line)
        current_sides = parse_total_sides(current_line)
        if side not in open_sides or side not in current_sides:
            return None
        return round(current_sides[side]["price"] - open_sides[side]["price"], 1)

    return None


def calculate_reverse_movement(market, recommended_pick, line_move, price_move):
    if market == "Moneyline":
        return price_move is not None and price_move > 0

    if market == "Spread":
        if line_move is None:
            return False
        return line_move < 0

    if market == "Total":
        if line_move is None:
            return False
        if recommended_pick.startswith("Over"):
            return line_move > 0
        if recommended_pick.startswith("Under"):
            return line_move < 0

    return False


def calculate_timing_strength(commence_time_display):
    dt = pd.to_datetime(commence_time_display, errors="coerce")
    if pd.isna(dt):
        return 50.0

    now_et = pd.Timestamp.now(tz=EASTERN_TZ)
    game_et = dt.tz_localize(EASTERN_TZ) if dt.tzinfo is None else dt.tz_convert(EASTERN_TZ)
    hours_to_game = max((game_et - now_et).total_seconds() / 3600, 0)
    return round(clamp(100 - (min(hours_to_game, 12) / 12) * 100, 0, 100), 1)


def calculate_signal_score(edge, line_move, reverse_movement, timing_strength):
    edge_strength = clamp(50 + edge * 5, 0, 100)
    line_strength = clamp(abs(line_move or 0) * 20, 0, 100)
    reverse_strength = 100 if reverse_movement else 0
    timing_strength = clamp(timing_strength, 0, 100)

    score = (
        edge_strength * 0.4
        + line_strength * 0.3
        + reverse_strength * 0.2
        + timing_strength * 0.1
    )
    return round(clamp(score, 0, 100), 1)


def normalize_signal_scores(df):
    if df.empty:
        return df

    data = df.copy()
    raw_scores = pd.to_numeric(data["Signal Score"], errors="coerce")

    for market in data["Market"].dropna().unique():
        mask = data["Market"] == market
        market_scores = raw_scores[mask]

        if market_scores.dropna().empty:
            continue

        min_score = market_scores.min()
        max_score = market_scores.max()

        if min_score == max_score:
            data.loc[mask, "Signal Score"] = 50.0
        else:
            normalized = 30 + ((market_scores - min_score) / (max_score - min_score)) * 70
            data.loc[mask, "Signal Score"] = normalized.round(1)

    return data


def calculate_confidence(model_probability):
    return round(clamp(model_probability * 100, 0, 100), 1)


def build_base_row(game_id, game, commence_time, market, sportsbook, sportsbook_line, recommended_pick, confidence, edge):
    return {
        "Game ID": game_id,
        "Game": game,
        "Commence Time": commence_time,
        "Market": market,
        "Sportsbook": sportsbook,
        "Sportsbook Line": sportsbook_line,
        "Recommended Pick": recommended_pick,
        "Confidence": f"{confidence:.1f}%",
        "Edge": f"{edge:.1f}%",
        "Open Line": sportsbook_line,
        "Current Line": sportsbook_line,
        "Line Move": 0.0,
        "Price Move": 0.0,
        "Reverse Movement": False,
        "Signal Score": 0.0,
    }


def build_moneyline_row(game_id, game, commence_time, sportsbook, away_team, home_team, market):
    prices = {}
    for outcome in market.get("outcomes", []):
        name = outcome.get("name")
        price = outcome.get("price")
        if name and price is not None:
            prices[name] = float(price)

    away_price = prices.get(away_team)
    home_price = prices.get(home_team)
    if away_price is None or home_price is None:
        return None

    away_vig_free, home_vig_free, away_implied, home_implied = vig_free_probabilities(away_price, home_price)
    price_gap = abs(home_implied - away_implied)
    home_model = clamp(0.5 + 0.03 + (home_vig_free - away_vig_free) * 0.55 + price_gap * 0.12, 0.05, 0.95)
    away_model = 1 - home_model
    home_edge = (home_model - home_implied) * 100
    away_edge = (away_model - away_implied) * 100

    if home_edge >= away_edge:
        recommended_pick = home_team
        confidence = calculate_confidence(home_model)
        edge = home_edge
    else:
        recommended_pick = away_team
        confidence = calculate_confidence(away_model)
        edge = away_edge

    sportsbook_line = f"{away_team} {format_american_odds(away_price)} vs {home_team} {format_american_odds(home_price)}"
    return build_base_row(game_id, game, commence_time, "Moneyline", sportsbook, sportsbook_line, recommended_pick, confidence, round(edge, 1))


def build_spread_row(game_id, game, commence_time, sportsbook, away_team, home_team, market):
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

    away_vig_free, home_vig_free, away_implied, home_implied = vig_free_probabilities(
        away_side["price"], home_side["price"]
    )

    home_spread = float(home_side["point"])
    away_spread = float(away_side["point"])
    projected_home_margin = (-home_spread) + (home_vig_free - away_vig_free) * 4.0
    home_cover_delta = projected_home_margin - (-home_spread)
    away_cover_delta = -home_cover_delta

    home_model = clamp(0.5 + home_cover_delta / 10.0 + (home_vig_free - 0.5) * 0.2, 0.05, 0.95)
    away_model = clamp(1 - home_model, 0.05, 0.95)
    home_edge = (home_model - home_implied) * 100
    away_edge = (away_model - away_implied) * 100

    if home_edge >= away_edge:
        recommended_pick = f"{home_team} {format_point(home_spread)}"
        confidence = calculate_confidence(home_model)
        edge = home_edge
    else:
        recommended_pick = f"{away_team} {format_point(away_spread)}"
        confidence = calculate_confidence(away_model)
        edge = away_edge

    sportsbook_line = (
        f"{away_team} {format_point(away_spread)} ({format_american_odds(away_side['price'])}) vs "
        f"{home_team} {format_point(home_spread)} ({format_american_odds(home_side['price'])})"
    )
    return build_base_row(game_id, game, commence_time, "Spread", sportsbook, sportsbook_line, recommended_pick, confidence, round(edge, 1))


def build_total_row(game_id, game, commence_time, sportsbook, market):
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

    over_vig_free, under_vig_free, over_implied, under_implied = vig_free_probabilities(
        over_side["price"], under_side["price"]
    )

    current_total = float(over_side["point"])
    projected_total = current_total + (over_vig_free - under_vig_free) * 5.0
    total_gap = projected_total - current_total
    over_model = clamp(0.5 + total_gap / 10.0 + (over_vig_free - 0.5) * 0.15, 0.05, 0.95)
    under_model = clamp(1 - over_model, 0.05, 0.95)
    over_edge = (over_model - over_implied) * 100
    under_edge = (under_model - under_implied) * 100

    if over_edge > under_edge:
        recommended_pick = f"Over {current_total:g}"
        confidence = calculate_confidence(over_model)
        edge = over_edge
    elif under_edge > over_edge:
        recommended_pick = f"Under {current_total:g}"
        confidence = calculate_confidence(under_model)
        edge = under_edge
    elif over_vig_free >= under_vig_free:
        recommended_pick = f"Over {current_total:g}"
        confidence = calculate_confidence(over_model)
        edge = over_edge
    else:
        recommended_pick = f"Under {current_total:g}"
        confidence = calculate_confidence(under_model)
        edge = under_edge

    sportsbook_line = (
        f"Over {current_total:g} ({format_american_odds(over_side['price'])}) vs "
        f"Under {current_total:g} ({format_american_odds(under_side['price'])})"
    )
    return build_base_row(game_id, game, commence_time, "Total", sportsbook, sportsbook_line, recommended_pick, confidence, round(edge, 1))


def enrich_signal_columns(row, snapshots_df):
    history = get_snapshot_history(snapshots_df, row["Game ID"], row["Market"], row["Sportsbook"])
    open_line, current_line = get_open_and_current_line(history, row["Current Line"])
    line_move = calculate_line_move(row["Market"], row["Recommended Pick"], open_line, current_line)
    price_move = calculate_price_move(row["Market"], row["Recommended Pick"], open_line, current_line)
    edge_value = float(str(row["Edge"]).replace("%", ""))
    reverse_movement = calculate_reverse_movement(row["Market"], row["Recommended Pick"], line_move, price_move)
    timing_strength = calculate_timing_strength(row["Commence Time"])
    signal_score = calculate_signal_score(edge_value, line_move, price_move, reverse_movement, timing_strength)

    row["Open Line"] = open_line
    row["Current Line"] = current_line
    row["Line Move"] = 0.0 if line_move is None else round(line_move, 1)
    row["Price Move"] = 0.0 if price_move is None else round(price_move, 1)
    row["Reverse Movement"] = reverse_movement
    row["Signal Score"] = signal_score
    return row


def build_predictions():
    raw_events = get_live_odds()
    future_events = filter_future_events(raw_events)
    snapshots_df = load_snapshots()

    rows = []
    snapshot_rows = []

    for event in future_events:
        away_team = event.get("away_team")
        home_team = event.get("home_team")
        commence_time_utc = parse_commence_time(event.get("commence_time"))

        if not away_team or not home_team or commence_time_utc is None:
            continue

        bookmaker = get_preferred_bookmaker(event)
        if not bookmaker:
            continue

        sportsbook = bookmaker.get("title", bookmaker.get("key", ""))
        game = f"{away_team} at {home_team}"
        game_id = build_game_id(away_team, home_team, commence_time_utc)
        commence_time_display = format_commence_time(commence_time_utc)

        moneyline_market = get_market(bookmaker, "h2h")
        if moneyline_market:
            row = build_moneyline_row(game_id, game, commence_time_display, sportsbook, away_team, home_team, moneyline_market)
            if row:
                row = enrich_signal_columns(row, snapshots_df)
                rows.append(row)
                snapshot_rows.append(row.copy())

        spread_market = get_market(bookmaker, "spreads")
        if spread_market:
            row = build_spread_row(game_id, game, commence_time_display, sportsbook, away_team, home_team, spread_market)
            if row:
                row = enrich_signal_columns(row, snapshots_df)
                rows.append(row)
                snapshot_rows.append(row.copy())

        total_market = get_market(bookmaker, "totals")
        if total_market:
            row = build_total_row(game_id, game, commence_time_display, sportsbook, total_market)
            if row:
                row = enrich_signal_columns(row, snapshots_df)
                rows.append(row)
                snapshot_rows.append(row.copy())

    result = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    result = normalize_signal_scores(result)

    if not result.empty:
        save_snapshots(result[OUTPUT_COLUMNS])
    else:
        print("No future games are available after filtering")

    return result


def predict_today():
    return build_predictions()


if __name__ == "__main__":
    print(predict_today().to_string(index=False))
