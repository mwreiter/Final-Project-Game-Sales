import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def get_avg_precip_and_holidays_all_months(db_path="holidays.db", output_file="monthly_precipitation_report.txt"):
    """
    Calculates average precipitation and lists holidays for all months found in the database.
    Saves the results to a single text file and prints them to the console.
    Also generates a bar chart of average precipitation per month.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Get unique months from the holiday_weather table
    cur.execute("SELECT DISTINCT strftime('%m', date) as month FROM holiday_weather ORDER BY month")
    months = [row[0] for row in cur.fetchall()]

    all_lines = []
    month_precip = {}

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
            month_precip[month] = avg_precip  
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

    # --- Plot bar chart ---
    if month_precip:
        months = list(month_precip.keys())
        values = list(month_precip.values())

        plt.figure(figsize=(12, 6))
        plt.bar(months, values, color='skyblue', edgecolor='black')
        plt.title("Average Holiday Precipitation by Month")
        plt.ylabel("Avg Precipitation (mm)")
        plt.xlabel("Month")
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig("monthly_holiday_precip_chart.png")
        plt.show()

        print("Chart saved as monthly_holiday_precip_chart.png")

# Run the function
if __name__ == "__main__":
    get_avg_precip_and_holidays_all_months()

