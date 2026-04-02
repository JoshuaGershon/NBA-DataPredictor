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
REFRESH_START_HOUR_ET = 12
REFRESH_END_HOUR_ET = 22

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


def parse_commence_time_utc(commence_time):
    if not commence_time:
        return None
    try:
        return datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
    except ValueError:
        return None


def format_commence_time_et(commence_time_utc):
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


def calculate_vig_free_probabilities(price_a, price_b):
    implied_a = american_to_implied_probability(price_a)
    implied_b = american_to_implied_probability(price_b)
    total = implied_a + implied_b
    if total == 0:
        return 0.5, 0.5, implied_a, implied_b
    return implied_a / total, implied_b / total, implied_a, implied_b


def format_american_odds(odds):
    return f"{int(float(odds)):+}"


def format_point(point):
    point = float(point)
    if point > 0:
        return f"+{point:g}"
    return f"{point:g}"


def build_game_id(away_team, home_team, commence_time_utc):
    stamp = commence_time_utc.strftime("%Y%m%dT%H%M%SZ") if commence_time_utc else "unknown"
    return f"{away_team}_{home_team}_{stamp}"


def now_utc():
    return datetime.now(timezone.utc)


def now_et():
    return now_utc().astimezone(EASTERN_TZ)


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
        "last_fetch_time": now_utc().isoformat(),
        "source": "API",
        "events": events,
    }
    with CACHE_FILE.open("w", encoding="utf-8") as f:
        json.dump(payload, f)


def parse_fetch_time(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def count_future_events(events, reference_utc=None):
    reference_utc = reference_utc or now_utc()
    total = 0
    for event in events or []:
        commence_time = parse_commence_time_utc(event.get("commence_time"))
        if commence_time and commence_time > reference_utc:
            total += 1
    return total


def debug_event_times(events):
    return [event.get("commence_time") for event in (events or [])[:3]]


def within_refresh_window(current_et):
    return REFRESH_START_HOUR_ET <= current_et.hour < REFRESH_END_HOUR_ET


def fetched_this_hour(last_fetch_time, current_et):
    if last_fetch_time is None:
        return False
    fetch_et = last_fetch_time.astimezone(EASTERN_TZ)
    return (
        fetch_et.year == current_et.year
        and fetch_et.month == current_et.month
        and fetch_et.day == current_et.day
        and fetch_et.hour == current_et.hour
    )


def fetch_api_events():
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
    return response.json()


def get_live_odds():
    current_utc = now_utc()
    current_et = current_utc.astimezone(EASTERN_TZ)
    cache = load_cache() or {}
    cached_events = cache.get("events", [])
    last_fetch_time = parse_fetch_time(cache.get("last_fetch_time"))
    cached_future_count = count_future_events(cached_events, current_utc)
    stale_cache = bool(cached_events) and cached_future_count == 0

    print(f"Current UTC time: {current_utc.isoformat()}")
    print(f"Current ET time: {current_et.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")
    print(f"Stale cache was rejected: {stale_cache}")

    should_call_api = False
    if API_KEY:
        if not cached_events:
            should_call_api = True
        elif stale_cache:
            should_call_api = True
        elif within_refresh_window(current_et) and not fetched_this_hour(last_fetch_time, current_et):
            should_call_api = True

    if should_call_api:
        try:
            api_events = fetch_api_events()
            print("Source used: API")
            print(f"Number of raw API events: {len(api_events)}")
            print(f"First 3 commence_time values: {debug_event_times(api_events)}")
            save_cache(api_events)
            return api_events, "API"
        except Exception as exc:
            print(f"API request failed: {exc}")
            if cached_future_count > 0:
                print("Source used: CACHE")
                print(f"Number of raw API events: {len(cached_events)}")
                print(f"First 3 commence_time values: {debug_event_times(cached_events)}")
                return cached_events, "CACHE"
            print("Source used: API")
            print("Number of raw API events: 0")
            print("First 3 commence_time values: []")
            return [], "API"

    if cached_events:
        print("Source used: CACHE")
        print(f"Number of raw API events: {len(cached_events)}")
        print(f"First 3 commence_time values: {debug_event_times(cached_events)}")
        return cached_events, "CACHE"

    print("Source used: API")
    print("Number of raw API events: 0")
    print("First 3 commence_time values: []")
    return [], "API"


def filter_future_events(events):
    now_utc_value = datetime.now(timezone.utc)
    future_events = []

    print(f"Raw API event count: {len(events)}")
    print(f"Current UTC time: {now_utc_value.isoformat()}")
    print(f"First 3 raw commence_time values: {[event.get('commence_time') for event in events[:3]]}")

    for event in events:
        commence_time = event.get("commence_time")
        if not commence_time:
            continue

        try:
            game_time_utc = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
        except ValueError:
            continue

        if game_time_utc > now_utc_value:
            future_events.append(event)

    print(f"Future event count after filtering: {len(future_events)}")
    if events and not future_events:
        print("Raw API/cache events existed, but all were filtered out as past games")
        print(f"Current UTC time: {now_utc_value.isoformat()}")
        print(f"First 3 raw commence_time values: {[event.get('commence_time') for event in events[:3]]}")

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
    snapshot_df.insert(0, "timestamp", now_utc().isoformat())
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
            if bookmaker.get("markets"):
                return bookmaker

    for bookmaker in bookmakers:
        if bookmaker.get("markets"):
            return bookmaker

    return None


def get_market(bookmaker, market_key):
    for market in bookmaker.get("markets", []):
        if market.get("key") == market_key:
            return market
    return None


def get_snapshot_history(snapshots_df, game_id, market, sportsbook):
    if snapshots_df.empty:
        return pd.DataFrame()

    required = {"Game ID", "Market", "Sportsbook"}
    if not required.issubset(set(snapshots_df.columns)):
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


def parse_moneyline_prices(line_text):
    try:
        left, right = line_text.split(" vs ")
        left_parts = left.rsplit(" ", 1)
        right_parts = right.rsplit(" ", 1)
        return {
            left_parts[0]: float(left_parts[1]),
            right_parts[0]: float(right_parts[1]),
        }
    except Exception:
        return {}


def parse_spread_sides(line_text):
    try:
        left, right = line_text.split(" vs ")
        left_name = left.split(" (")[0]
        right_name = right.split(" (")[0]
        left_team, left_point = left_name.rsplit(" ", 1)
        right_team, right_point = right_name.rsplit(" ", 1)
        left_price = float(left.split("(")[1].split(")")[0])
        right_price = float(right.split("(")[1].split(")")[0])
        return {
            left_team: {"point": float(left_point), "price": left_price},
            right_team: {"point": float(right_point), "price": right_price},
        }
    except Exception:
        return {}


def parse_total_sides(line_text):
    try:
        left, right = line_text.split(" vs ")
        left_name = left.split(" (")[0]
        right_name = right.split(" (")[0]
        left_side, left_total = left_name.split(" ", 1)
        right_side, right_total = right_name.split(" ", 1)
        left_price = float(left.split("(")[1].split(")")[0])
        right_price = float(right.split("(")[1].split(")")[0])
        return {
            left_side: {"point": float(left_total), "price": left_price},
            right_side: {"point": float(right_total), "price": right_price},
        }
    except Exception:
        return {}


def get_open_line(history, current_line):
    if history.empty or "Sportsbook Line" not in history.columns:
        return current_line
    return history.iloc[0]["Sportsbook Line"]


def calculate_line_move(market, recommended_pick, open_line, current_line):
    if not open_line or not current_line:
        return 0.0

    if market == "Moneyline":
        return 0.0

    if market == "Spread":
        team = " ".join(recommended_pick.split(" ")[:-1])
        open_sides = parse_spread_sides(open_line)
        current_sides = parse_spread_sides(current_line)
        if team not in open_sides or team not in current_sides:
            return 0.0
        return round(current_sides[team]["point"] - open_sides[team]["point"], 1)

    if market == "Total":
        side = recommended_pick.split(" ")[0]
        open_sides = parse_total_sides(open_line)
        current_sides = parse_total_sides(current_line)
        if side not in open_sides or side not in current_sides:
            return 0.0
        return round(current_sides[side]["point"] - open_sides[side]["point"], 1)

    return 0.0


def calculate_price_move(market, recommended_pick, open_line, current_line):
    if not open_line or not current_line:
        return 0.0

    if market == "Moneyline":
        open_prices = parse_moneyline_prices(open_line)
        current_prices = parse_moneyline_prices(current_line)
        if recommended_pick not in open_prices or recommended_pick not in current_prices:
            return 0.0
        return round(current_prices[recommended_pick] - open_prices[recommended_pick], 1)

    if market == "Spread":
        team = " ".join(recommended_pick.split(" ")[:-1])
        open_sides = parse_spread_sides(open_line)
        current_sides = parse_spread_sides(current_line)
        if team not in open_sides or team not in current_sides:
            return 0.0
        return round(current_sides[team]["price"] - open_sides[team]["price"], 1)

    if market == "Total":
        side = recommended_pick.split(" ")[0]
        open_sides = parse_total_sides(open_line)
        current_sides = parse_total_sides(current_line)
        if side not in open_sides or side not in current_sides:
            return 0.0
        return round(current_sides[side]["price"] - open_sides[side]["price"], 1)

    return 0.0


def calculate_reverse_movement(market, recommended_pick, line_move, price_move):
    if market == "Moneyline":
        return price_move > 0

    if market == "Spread":
        selected_point = float(recommended_pick.rsplit(" ", 1)[1])
        if selected_point > 0:
            return line_move < 0
        return line_move > 0

    if market == "Total":
        if recommended_pick.startswith("Over"):
            return line_move < 0 or price_move > 0
        return line_move > 0 or price_move > 0

    return False


def calculate_timing_strength(commence_time_utc):
    if commence_time_utc is None:
        return 50.0
    hours_to_game = max((commence_time_utc - now_utc()).total_seconds() / 3600, 0)
    return round(clamp(100 - (min(hours_to_game, 24) / 24) * 100, 0, 100), 1)


def calculate_signal_score(edge, line_move, reverse_movement, timing_strength):
    edge_strength = clamp(abs(edge) * 12.5, 0, 100)
    movement_strength = clamp(abs(line_move) * 25, 0, 100)
    reverse_strength = 100 if reverse_movement else 0
    score = (
        edge_strength * 0.45
        + movement_strength * 0.25
        + reverse_strength * 0.15
        + timing_strength * 0.15
    )
    return round(clamp(score, 0, 100), 1)


def calculate_confidence(vig_free_probability, edge, line_move):
    score = vig_free_probability * 100
    score += min(abs(edge) * 4, 12)
    score += min(abs(line_move) * 6, 8)
    return round(clamp(score, 35, 75), 1)


def build_base_row(game_id, game, commence_time_display, market, sportsbook, sportsbook_line, recommended_pick, confidence, edge):
    return {
        "Game ID": game_id,
        "Game": game,
        "Commence Time": commence_time_display,
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


def extract_moneyline_prices(market):
    prices = {}
    for outcome in market.get("outcomes", []):
        name = outcome.get("name")
        price = outcome.get("price")
        if name and price is not None:
            prices[name] = float(price)
    return prices


def build_moneyline_row(game_id, game, commence_time_display, sportsbook, away_team, home_team, market):
    prices = extract_moneyline_prices(market)
    away_price = prices.get(away_team)
    home_price = prices.get(home_team)
    if away_price is None or home_price is None:
        return None

    away_vig_free, home_vig_free, away_implied, home_implied = calculate_vig_free_probabilities(away_price, home_price)
    price_gap = abs(home_implied - away_implied)

    home_model = clamp(
        0.5
        + (home_vig_free - away_vig_free) * 0.55
        + price_gap * 0.10
        + 0.02,
        0.08,
        0.92,
    )
    away_model = 1 - home_model

    home_edge = round((home_model - home_implied) * 100, 1)
    away_edge = round((away_model - away_implied) * 100, 1)

    if home_edge >= away_edge:
        recommended_pick = home_team
        confidence = calculate_confidence(home_vig_free, home_edge, 0.0)
        edge = home_edge
    else:
        recommended_pick = away_team
        confidence = calculate_confidence(away_vig_free, away_edge, 0.0)
        edge = away_edge

    sportsbook_line = f"{away_team} {format_american_odds(away_price)} vs {home_team} {format_american_odds(home_price)}"
    return build_base_row(game_id, game, commence_time_display, "Moneyline", sportsbook, sportsbook_line, recommended_pick, confidence, edge)


def extract_spread_sides(market):
    sides = {}
    for outcome in market.get("outcomes", []):
        name = outcome.get("name")
        price = outcome.get("price")
        point = outcome.get("point")
        if name and price is not None and point is not None:
            sides[name] = {"price": float(price), "point": float(point)}
    return sides


def build_spread_row(game_id, game, commence_time_display, sportsbook, away_team, home_team, market):
    sides = extract_spread_sides(market)
    away_side = sides.get(away_team)
    home_side = sides.get(home_team)
    if away_side is None or home_side is None:
        return None

    away_vig_free, home_vig_free, away_implied, home_implied = calculate_vig_free_probabilities(
        away_side["price"], home_side["price"]
    )
    projected_home_margin = -home_side["point"] + (home_vig_free - away_vig_free) * 3.5
    home_cover_signal = projected_home_margin - (-home_side["point"])
    away_cover_signal = -home_cover_signal

    home_model = clamp(0.5 + home_cover_signal / 10 + (home_vig_free - 0.5) * 0.10, 0.08, 0.92)
    away_model = 1 - home_model

    home_edge = round((home_model - home_implied) * 100, 1)
    away_edge = round((away_model - away_implied) * 100, 1)

    if home_edge >= away_edge:
        recommended_pick = f"{home_team} {format_point(home_side['point'])}"
        confidence = calculate_confidence(home_vig_free, home_edge, home_cover_signal)
        edge = home_edge
    else:
        recommended_pick = f"{away_team} {format_point(away_side['point'])}"
        confidence = calculate_confidence(away_vig_free, away_edge, away_cover_signal)
        edge = away_edge

    sportsbook_line = (
        f"{away_team} {format_point(away_side['point'])} ({format_american_odds(away_side['price'])}) vs "
        f"{home_team} {format_point(home_side['point'])} ({format_american_odds(home_side['price'])})"
    )
    return build_base_row(game_id, game, commence_time_display, "Spread", sportsbook, sportsbook_line, recommended_pick, confidence, edge)


def extract_total_sides(market):
    sides = {}
    for outcome in market.get("outcomes", []):
        name = outcome.get("name")
        price = outcome.get("price")
        point = outcome.get("point")
        if name and price is not None and point is not None:
            sides[name] = {"price": float(price), "point": float(point)}
    return sides


def build_total_row(game_id, game, commence_time_display, sportsbook, market):
    sides = extract_total_sides(market)
    over_side = sides.get("Over")
    under_side = sides.get("Under")
    if over_side is None or under_side is None:
        return None

    over_vig_free, under_vig_free, over_implied, under_implied = calculate_vig_free_probabilities(
        over_side["price"], under_side["price"]
    )
    total_number = float(over_side["point"])
    total_signal = (over_vig_free - under_vig_free) * 5.0

    over_model = clamp(0.5 + total_signal / 10 + (over_vig_free - 0.5) * 0.08, 0.08, 0.92)
    under_model = 1 - over_model

    over_edge = round((over_model - over_implied) * 100, 1)
    under_edge = round((under_model - under_implied) * 100, 1)

    if over_edge > under_edge:
        recommended_pick = f"Over {total_number:g}"
        confidence = calculate_confidence(over_vig_free, over_edge, total_signal)
        edge = over_edge
    elif under_edge > over_edge:
        recommended_pick = f"Under {total_number:g}"
        confidence = calculate_confidence(under_vig_free, under_edge, total_signal)
        edge = under_edge
    elif over_vig_free >= under_vig_free:
        recommended_pick = f"Over {total_number:g}"
        confidence = calculate_confidence(over_vig_free, over_edge, total_signal)
        edge = over_edge
    else:
        recommended_pick = f"Under {total_number:g}"
        confidence = calculate_confidence(under_vig_free, under_edge, total_signal)
        edge = under_edge

    sportsbook_line = (
        f"Over {total_number:g} ({format_american_odds(over_side['price'])}) vs "
        f"Under {total_number:g} ({format_american_odds(under_side['price'])})"
    )
    return build_base_row(game_id, game, commence_time_display, "Total", sportsbook, sportsbook_line, recommended_pick, confidence, edge)


def enrich_signal_columns(row, snapshots_df, commence_time_utc):
    history = get_snapshot_history(snapshots_df, row["Game ID"], row["Market"], row["Sportsbook"])
    open_line = get_open_line(history, row["Current Line"])
    current_line = row["Current Line"]
    line_move = calculate_line_move(row["Market"], row["Recommended Pick"], open_line, current_line)
    price_move = calculate_price_move(row["Market"], row["Recommended Pick"], open_line, current_line)
    reverse_movement = calculate_reverse_movement(row["Market"], row["Recommended Pick"], line_move, price_move)
    edge_value = float(str(row["Edge"]).replace("%", ""))
    timing_strength = calculate_timing_strength(commence_time_utc)
    signal_score = calculate_signal_score(edge_value, line_move, reverse_movement, timing_strength)

    row["Open Line"] = open_line
    row["Current Line"] = current_line
    row["Line Move"] = round(line_move, 1)
    row["Price Move"] = round(price_move, 1)
    row["Reverse Movement"] = bool(reverse_movement)
    row["Signal Score"] = signal_score
    return row


def build_predictions():
    raw_events, _source = get_live_odds()
    future_events = filter_future_events(raw_events)
    snapshots_df = load_snapshots()

    rows = []

    for event in future_events:
        away_team = event.get("away_team")
        home_team = event.get("home_team")
        commence_time_utc = parse_commence_time_utc(event.get("commence_time"))

        if not away_team or not home_team or commence_time_utc is None:
            continue

        bookmaker = get_preferred_bookmaker(event)
        if bookmaker is None:
            continue

        sportsbook = bookmaker.get("title", bookmaker.get("key", ""))
        game = f"{away_team} at {home_team}"
        game_id = build_game_id(away_team, home_team, commence_time_utc)
        commence_time_display = format_commence_time_et(commence_time_utc)

        market_builders = [
            ("h2h", lambda market: build_moneyline_row(game_id, game, commence_time_display, sportsbook, away_team, home_team, market)),
            ("spreads", lambda market: build_spread_row(game_id, game, commence_time_display, sportsbook, away_team, home_team, market)),
            ("totals", lambda market: build_total_row(game_id, game, commence_time_display, sportsbook, market)),
        ]

        for market_key, builder in market_builders:
            market = get_market(bookmaker, market_key)
            if market is None:
                continue

            row = builder(market)
            if row is None:
                continue

            rows.append(enrich_signal_columns(row, snapshots_df, commence_time_utc))

    result = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

    if result.empty:
        print("No future games are available after filtering")
        return result

    save_snapshots(result[OUTPUT_COLUMNS].drop_duplicates(subset=["Game ID", "Market", "Sportsbook", "Current Line"]))
    return result


def predict_today():
    return build_predictions()


if __name__ == "__main__":
    print(predict_today().to_string(index=False))
