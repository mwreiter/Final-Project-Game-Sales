import sqlite3
import matplotlib.pyplot as plt
import os 

def get_avg_precip_and_holidays_all_months(db_path="holidays.db", output_file="monthly_precipitation_report.txt"):
    """
    Joins the holidays and weather_daily tables to calculate average precipitation for each month (on holidays),
    lists holidays with precipitation info, saves a text report, and creates a visualization.
    """
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "holidays.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Get all months (as 2-digit strings)
    cur.execute("SELECT DISTINCT strftime('%m', holidays.date) as month FROM holidays ORDER BY month")
    months = [row[0] for row in cur.fetchall()]

    all_lines = []
    month_precip = {}

    for month in months:
        # Join holidays + weather for this month
        cur.execute('''
            SELECT holidays.date, holidays.name, weather_daily.precipitation_sum
            FROM holidays
            JOIN weather_daily ON holidays.date = weather_daily.date
            WHERE strftime('%m', holidays.date) = ?
            ORDER BY holidays.date
        ''', (month,))
        results = cur.fetchall()

        # Calculate average precipitation
        if results:
            total_precip = sum([r[2] for r in results if r[2] is not None])
            count = len([r[2] for r in results if r[2] is not None])
            avg_precip = round(total_precip / count, 2) if count > 0 else 0
            month_precip[month] = avg_precip
        else:
            avg_precip = 0
            month_precip[month] = 0

        # Build report lines
        all_lines.append(f"\nMonth {month} — Holidays and Precipitation:")
        if results:
            for date, name, precip in results:
                all_lines.append(f" - {date} | {name}: {precip} mm")
            all_lines.append(f"Average Precipitation: {avg_precip} mm\n")
        else:
            all_lines.append(" - No holidays with weather data found.\n")

    conn.close()

    # Print and save report
    for line in all_lines:
        print(line)

    with open(output_file, "w") as f:
        for line in all_lines:
            f.write(line + "\n")

    print(f"Full report saved to {output_file}")

    # Create visualization
    if month_precip:
        month_labels = list(month_precip.keys())
        values = list(month_precip.values())

        plt.figure(figsize=(12, 6))
        plt.bar(month_labels, values, color='skyblue', edgecolor='black')
        plt.title("Average Holiday Precipitation by Month")
        plt.ylabel("Avg Precipitation (mm)")
        plt.xlabel("Month")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("monthly_holiday_precip_chart.png")
        plt.show()

        print("Chart saved as monthly_holiday_precip_chart.png")

# Run the function
if __name__ == "__main__":
    get_avg_precip_and_holidays_all_months()
