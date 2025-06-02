"""import pandas as pd
import numpy as np

# Load CSV
df = pd.read_csv('expenses.csv')

# Clean 'amount' column (remove $, convert to float)
df['amount'] = df['amount'].replace('[\$,]', '', regex=True).astype(float)

# Convert 'date' column to datetime
df['date'] = pd.to_datetime(df['date'])

# Extract month (YYYY-MM format)
df['month'] = df['date'].dt.to_period('M')

# Group by month and category → total expenses
monthly_expense_summary = df.groupby(['month', 'category'])['amount'].sum().unstack().fillna(0)

# Group by month only → total and average per month
monthly_total = df.groupby('month')['amount'].sum()
monthly_average = df.groupby('month')['amount'].mean()

# Save cleaned data and summary
df.to_csv('cleaned_expenses.csv', index=False)
monthly_expense_summary.to_csv('monthly_summary_by_category.csv')

# Display outputs
print("\n✅ Cleaned Data Sample:\n", df.head())
print("\n📊 Monthly Category-Wise Breakdown:\n", monthly_expense_summary)
print("\n💰 Monthly Totals:\n", monthly_total)
print("\n📈 Monthly Averages:\n", monthly_average)"""

import pandas as pd
import numpy as np

# Load CSV
df = pd.read_csv('expenses.csv')

# Clean 'amount' column (remove $, convert to float)
df['amount'] = df['amount'].replace(r'[\$,]', '', regex=True).astype(float)

# Convert 'date' column to datetime
df['date'] = pd.to_datetime(df['date'])

# Extract month (YYYY-MM format)
df['month'] = df['date'].dt.to_period('M')

# Group by month and category → total expenses
monthly_expense_summary = df.groupby(['month', 'category'])['amount'].sum().unstack().fillna(0)

# Group by month only → total and average per month
monthly_total = df.groupby('month')['amount'].sum()
monthly_average = df.groupby('month')['amount'].mean()

# Save cleaned data and summary
df.to_csv('cleaned_expenses.csv', index=False)
monthly_expense_summary.to_csv('monthly_summary_by_category.csv')

# Display outputs
print("\n✅ Cleaned Data Sample:\n", df.head())
print("\n📊 Monthly Category-Wise Breakdown:\n", monthly_expense_summary)
print("\n💰 Monthly Totals:\n", monthly_total)
print("\n📈 Monthly Averages:\n", monthly_average)

