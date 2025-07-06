import pandas as pd

def load_data():
    data = {
        'device_id': ['AC01', 'WM02', 'FR03'],
        'date': ['2025-07-01'] * 3,
        'kwh_used': [8.5, 12.4, 9.3]
    }
    return pd.DataFrame(data)

def transform_data(df):
    return df[df['kwh_used'] > 10]

def alert_high_usage(df):
    for _, row in df.iterrows():
        print(f"ALERT: {row['device_id']} used {row['kwh_used']} kWh on {row['date']}!")

def run_elt():
    df = load_data()
    high_usage = transform_data(df)
    alert_high_usage(high_usage)

if __name__ == "__main__":
    run_elt()
