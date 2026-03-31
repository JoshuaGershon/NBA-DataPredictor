def american_to_implied_probability(odds):
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def calculate_edge(model_probability, american_odds):
    implied_probability = american_to_implied_probability(american_odds)
    return model_probability - implied_probability


def rank_best_bets(df, probability_col="model_probability", odds_col="american_odds", top_n=10):
    data = df.copy()
    data["implied_probability"] = data[odds_col].apply(american_to_implied_probability)
    data["edge"] = data[probability_col] - data["implied_probability"]
    return data.sort_values("edge", ascending=False).head(top_n).reset_index(drop=True)
