import sqlite3
import pandas as pd

def calculate_avg_temp_by_week():
    # Connect to the existing holidays.db
    conn = sqlite3.connect("holidays.db")
    cur = conn.cursor()

    print("🧠 Calculating average max temperature per week (Jan–May)...")

    # Query for weather + holiday matches (LEFT JOIN to keep all weather dates)
    query = '''
    SELECT w.date, w.temperature_max, h.name AS holiday_name
    FROM weather_daily w
    LEFT JOIN holidays h ON w.date = h.date
    WHERE w.date BETWEEN '2024-01-01' AND '2024-05-31'
    ORDER BY w.date ASC
    '''
    df = pd.read_sql_query(query, conn)

    if df.empty:
        print("⚠️ No weather data available for Jan–May.")
        return

    # Convert to datetime and get week numbers
    df['date'] = pd.to_datetime(df['date'])
    df['week'] = df['date'].dt.isocalendar().week
    df['year'] = df['date'].dt.year

    # Group by year & week, calculate avg temp + collect holidays
    grouped = df.groupby(['year', 'week']).agg({
        'temperature_max': 'mean',
        'holiday_name': lambda x: ', '.join(x.dropna().unique())
    }).reset_index()

    # Rename columns
    grouped.rename(columns={'temperature_max': 'avg_temp_max', 'holiday_name': 'holidays'}, inplace=True)

    # Write to .txt
    with open("avg_weekly_temperature.txt", "w") as f:
        f.write("Year\tWeek\tAvg Max Temp (°C)\tHoliday(s)\n")
        for _, row in grouped.iterrows():
            f.write(f"{row['year']}\t{row['week']}\t{row['avg_temp_max']:.2f}\t{row['holidays'] or '-'}\n")

    print("✅ Wrote analysis to avg_weekly_temperature.txt")

    conn.close()

# Run the function
if __name__ == "__main__":
    calculate_avg_temp_by_week()
