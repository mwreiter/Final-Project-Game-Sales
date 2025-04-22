import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

def calculate_avg_temp_by_month():
   # Connect to the existing holidays.db
   script_dir = os.path.dirname(os.path.abspath(__file__))
   db_path = os.path.join(script_dir, "holidays.db")
     
   conn = sqlite3.connect(db_path)
   cur = conn.cursor()


   print("Calculating average max temperature per month (Jan–Dec 2024)...")


   # Query for weather + holiday matches
   query = '''
   SELECT w.date, w.temperature_max
   FROM weather_daily w
   INNER JOIN holidays h ON w.date = h.date
   WHERE h.date BETWEEN '2024-01-01' AND '2024-12-31'
   ORDER BY w.date ASC
   '''
   df = pd.read_sql_query(query, conn)


   if df.empty:
       print("No holiday weather data available for 2024.")
       return


   # Convert to datetime and extract year/month
   df['date'] = pd.to_datetime(df['date'])
   df['month'] = df['date'].dt.month
   df['year'] = df['date'].dt.year


   # Group by year and month
   grouped = df.groupby(['year', 'month']).agg({
       'temperature_max': 'mean'
   }).reset_index()


   # Rename for clarity
   grouped.rename(columns={'temperature_max': 'avg_temp_max'}, inplace=True)


   # Write results to text file
   with open("avg_monthly_temperature.txt", "w") as f:
       f.write("Year\tMonth\tAvg Max Temp (°C)\n")
       for _, row in grouped.iterrows():
           f.write(f"{row['year']}\t{row['month']}\t{row['avg_temp_max']:.2f}\n")


   print("Wrote analysis to avg_monthly_temperature.txt")


   # Plotting
   plt.figure(figsize=(10, 5))
   plt.bar(grouped['month'], grouped['avg_temp_max'], color='coral', edgecolor='black')
   import calendar
   plt.xticks(grouped['month'], [calendar.month_abbr[m] for m in grouped['month']])
   plt.title("Average Monthly Max Temp on Holidays (2024)")
   plt.xlabel("Month")
   plt.ylabel("Avg Max Temp on Holidays (°C)")
   plt.tight_layout()
   plt.savefig("avg_monthly_temp_bar.png")
   plt.show()


   print("Saved bar chart as avg_monthly_temp_bar.png")
   conn.close()


# Run the function
if __name__ == "__main__":
   calculate_avg_temp_by_month()

