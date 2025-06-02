import pandas as pd
import numpy as np

# Load the dataset
df = pd.read_csv("energyusage.csv")

# Convert timestamp to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

# Convert energy_kwh to float and clean invalid entries
df['energy_kwh'] = pd.to_numeric(df['energy_kwh'], errors='coerce')

# Drop rows with missing timestamp or energy values
df_cleaned = df.dropna(subset=['timestamp', 'energy_kwh'])

# Calculate total and average energy per device
device_summary = df_cleaned.groupby('device_id')['energy_kwh'].agg(['sum', 'mean'])

# Generate room-level total energy usage
room_summary = df_cleaned.groupby('room_id')['energy_kwh'].sum()

# Save cleaned dataset and summaries
df_cleaned.to_csv('cleaned_energy_logs.csv', index=False)
device_summary.to_csv('energy_summary_by_device.csv')
room_summary.to_csv('energy_summary_by_room.csv')

# Display results
print("\n✅ Cleaned Data Sample:\n", df_cleaned.head())
print("\n⚡ Device-Level Energy Summary:\n", device_summary)
print("\n🏠 Room-Level Energy Usage:\n", room_summary)
