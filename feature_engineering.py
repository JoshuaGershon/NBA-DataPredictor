import pandas as pd


def add_features(df):
    data = df.copy()
    data["date"] = pd.to_datetime(data["date"])
    data = data.sort_values(["team", "date"]).reset_index(drop=True)

    grouped = data.groupby("team", group_keys=False)

    prev_points_scored = grouped["points_scored"].shift(1)
    prev_points_allowed = grouped["points_allowed"].shift(1)
    prev_wins = grouped["win"].shift(1)
    prev_point_diff = grouped["point_diff"].shift(1)
    prev_dates = grouped["date"].shift(1)

    data["rolling_points_scored_last_5"] = (
        prev_points_scored.groupby(data["team"]).rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )

    data["rolling_points_allowed_last_5"] = (
        prev_points_allowed.groupby(data["team"]).rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )

    data["home_away_flag"] = data["home_away"].str.lower().eq("home").astype(int)

    data["days_of_rest"] = (data["date"] - prev_dates).dt.days
    data["back_to_back_flag"] = data["days_of_rest"].eq(1).astype(int)

    data["win_percentage_last_10"] = (
        prev_wins.groupby(data["team"]).rolling(10, min_periods=1).mean().reset_index(level=0, drop=True)
    )

    data["point_differential_last_5"] = (
        prev_point_diff.groupby(data["team"]).rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )

    return data
