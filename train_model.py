import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


FEATURE_COLS = [
    "avg_points_scored_last_5",
    "avg_points_allowed_last_5",
    "win_percentage_last_10",
    "home_away_flag",
]

TARGET_COL = "win"


def load_training_data(csv_path):
    return pd.read_csv(csv_path)


def prepare_training_data(df):
    data = df.copy()
    data = data.dropna(subset=FEATURE_COLS + [TARGET_COL])
    X = data[FEATURE_COLS]
    y = data[TARGET_COL].astype(int)
    return X, y


def train_model(X_train, y_train):
    model = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(max_iter=1000)),
        ]
    )
    model.fit(X_train, y_train)
    return model


def evaluate_model(model, X_test, y_test):
    predictions = model.predict(X_test)
    probabilities = model.predict_proba(X_test)[:, 1]

    print(f"Accuracy: {accuracy_score(y_test, predictions):.4f}")
    print(f"Log Loss: {log_loss(y_test, probabilities):.4f}")


def save_model(model, model_path="model.pkl"):
    joblib.dump(model, model_path)


if __name__ == "__main__":
    data = load_training_data("historical_features.csv")
    X, y = prepare_training_data(data)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    model = train_model(X_train, y_train)
    evaluate_model(model, X_test, y_test)
    save_model(model)
