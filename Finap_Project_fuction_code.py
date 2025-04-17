import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def calculate_avg_temp_by_week():
    # Connect to the existing holidays.db
    conn = sqlite3.connect("holidays.db")
    cur = conn.cursor()

    print("🧠 Calculating average max temperature per week (Jan–May)...")

    # Query for weather + holiday matches (LEFT JOIN to keep all weather dates)
    query = '''
    SELECT w.date, w.temperature_max, h.name AS holiday_name
    FROM weather_daily w
    INNER JOIN holidays h ON w.date = h.date
    WHERE h.date BETWEEN '2024-01-01' AND '2024-05-31'
    ORDER BY w.temperature_max DESC
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

    # --- 📊 BAR CHART: Hottest to Coldest Week ---
    # Sort from highest to lowest temperature
    sorted_df = grouped.sort_values(by='avg_temp_max', ascending=False)

    # Build x-axis labels as Week # with holiday(s) below
    sorted_df['label'] = sorted_df.apply(
        lambda row: f"Week {row['week']} ({row['year']})\n{row['holidays'] or '-'}", axis=1
    )

    plt.figure(figsize=(12, 6))
    plt.bar(sorted_df['label'], sorted_df['avg_temp_max'], color='tomato', edgecolor='black')
    plt.xticks(rotation=45, ha='right')
    plt.title("Average Weekly Max Temp (Jan–June 2024)\nSorted Hottest to Coldest")
    plt.ylabel("Avg Max Temp (°C)")
    plt.tight_layout()
    plt.savefig("avg_weekly_temp_bar_sorted.png")
    plt.show()

    print("✅ Saved bar chart as avg_weekly_temp_bar_sorted.png")

    conn.close()

if __name__ == "__main__":
    calculate_avg_temp_by_week()

def get_avg_precip_and_holidays_all_months(db_path="holidays.db", output_file="monthly_precipitation_report.txt"):
    """
    Calculates average precipitation and lists holidays for all months found in the database.
    Saves the results to a single text file and prints them to the console.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Get unique months from the holiday_weather table
    cur.execute("SELECT DISTINCT strftime('%m', date) as month FROM holiday_weather ORDER BY month")
    months = [row[0] for row in cur.fetchall()]

    all_lines = []

    for month in months:
        # Get average precipitation for the month
        cur.execute('''
            SELECT ROUND(AVG(precipitation_sum), 2)
            FROM holiday_weather
            WHERE strftime('%m', date) = ?
        ''', (month,))
        avg_precip = cur.fetchone()[0]

        # Get holidays for the month
        cur.execute('''
            SELECT date, name, precipitation_sum
            FROM holiday_weather
            WHERE strftime('%m', date) = ?
            ORDER BY date
        ''', (month,))
        holidays = cur.fetchall()

        # Build output
        all_lines.append(f"\n Month {month} — Holidays and Precipitation:")
        if holidays:
            for date, name, precip in holidays:
                all_lines.append(f" - {date} | {name}: {precip} mm")
            all_lines.append(f"Average Precipitation: {avg_precip} mm\n")
        else:
            all_lines.append(" - No holidays found in this month.\n")

    conn.close()

    # Print and save
    for line in all_lines:
        print(line)

    with open(output_file, "w") as f:
        for line in all_lines:
            f.write(line + "\n")

    print(f"\n Full monthly holiday precipitation report saved to {output_file}")

# Run the function
if __name__ == "__main__":
    calculate_avg_temp_by_week()
    get_avg_precip_and_holidays_all_months()

