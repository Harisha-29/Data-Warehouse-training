import pandas as pd
from pathlib import Path
from datetime import datetime

# ----- config -----
RAW_FILE   = Path("data/expenses_raw.csv")
OUT_FILE   = Path("monthly_summary.csv")
THRESHOLD  = 5000       
# -------------------

def load():
    return pd.read_csv(RAW_FILE, parse_dates=["date"])

def transform(df):
    df["year_month"] = df["date"].dt.to_period("M")
    summary = (
        df.groupby(["user_id", "year_month"])["amount"]
          .sum()
          .reset_index(name="total_spend")
          .sort_values(["year_month", "user_id"])
    )
    summary["savings_alert"] = summary["total_spend"] > THRESHOLD
    return summary

def save(df):
    df.to_csv(OUT_FILE, index=False)
    print(f" Wrote {OUT_FILE.resolve()}")

def alert(df):
    alerts = df[df["savings_alert"]]
    if alerts.empty:
        print(" No user exceeded the threshold this cycle.")
    else:
        print(" ALERT – users above threshold:")
        for _, row in alerts.iterrows():
            print(f" • User {row.user_id} spent {row.total_spend} on {row.year_month}")

def main():
    raw = load()
    summary = transform(raw)
    save(summary)
    alert(summary)

if __name__ == "__main__":
    main()
