### Names: Mason Reiter Noah Chang
### Email: mwreiter@umich.edu noahchan@umich.edu
### Notes: Calendar Holiday Umich Comparison Project

import sqlite3
import requests


# API setup for Calendarific
API_KEY = "2Cmq6y302J2wSAtLtdGBxYnkZoxDW4aV"
base_url = "https://calendarific.com/api/v2/holidays"
country = "US"
year = 2025

# Weather API setup
latitude = 42.2808  # Ann Arbor, MI
longitude = -83.7430
start_date = "2024-01-01"
end_date = "2024-01-10"  # Keep small range if testing
weather_url = "https://archive-api.open-meteo.com/v1/archive"

# Connect to shared database
conn = sqlite3.connect("holidays.db")
cur = conn.cursor()

# ----- HOLIDAYS TABLE -----
cur.execute('''
    CREATE TABLE IF NOT EXISTS holidays (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        description TEXT,
        date TEXT UNIQUE,
        month INTEGER
    )
''')

inserted_count = 0
max_insert = 25

for month in range(1, 13):
    if inserted_count >= max_insert:
        break

    params = {
        "api_key": API_KEY,
        "country": country,
        "year": year,
        "month": month,
    }

    try:
        response = requests.get(base_url, params=params)
        data = response.json()

        if "response" in data and "holidays" in data["response"]:
            holidays = data["response"]["holidays"]
            seen = set()

            for holiday in holidays:
                if inserted_count >= max_insert:
                    break

                name = holiday.get("name", "").strip()
                description = holiday.get("description", "").strip()
                date = holiday['date']['iso']

                if "bank" in name.lower() or "bank" in description.lower():
                    continue

                key = (name.lower(), date)
                if key in seen:
                    continue
                seen.add(key)

                cur.execute("SELECT 1 FROM holidays WHERE date = ?", (date,))
                if cur.fetchone():
                    continue

                cur.execute('''
                    INSERT INTO holidays (name, description, date, month)
                    VALUES (?, ?, ?, ?)
                ''', (name, description, date, month))
                inserted_count += 1

    except Exception as e:
        print(f"Error for month {month}:", e)

print(f"✅ Inserted {inserted_count} new holiday(s) into holidays.db")

# ----- DAILY WEATHER TABLE (25 rows per run) -----
cur.execute('''
    CREATE TABLE IF NOT EXISTS weather_daily (
        date TEXT PRIMARY KEY,
        temperature_max REAL,
        temperature_min REAL,
        wind_gust_max REAL,
        wind_speed_max REAL,
        apparent_temp_max REAL,
        apparent_temp_min REAL,
        precipitation_sum REAL,
        precipitation_hours REAL,
        precipitation_probability_max REAL
    )
''')

# Count how many are already inserted
cur.execute("SELECT COUNT(*) FROM weather_daily")
existing_rows = cur.fetchone()[0]

# Calculate the new date range
from datetime import datetime, timedelta

base_date = datetime.strptime("2024-01-01", "%Y-%m-%d")
start_date_dt = base_date + timedelta(days=existing_rows)
end_date_dt = start_date_dt + timedelta(days=24)

start_date = start_date_dt.strftime("%Y-%m-%d")
end_date = end_date_dt.strftime("%Y-%m-%d")

params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": start_date,
    "end_date": end_date,
    "daily": ",".join([
        "temperature_2m_max", "temperature_2m_min",
        "wind_gusts_10m_max", "wind_speed_10m_max",
        "apparent_temperature_max", "apparent_temperature_min",
        "precipitation_sum", "precipitation_hours",
        "precipitation_probability_max"
    ]),
    "timezone": "America/Chicago"
}

weather_inserted = 0

try:
    response = requests.get(weather_url, params=params)
    response.raise_for_status()
    data = response.json()
    dates = data["daily"]["time"]

    for i in range(len(dates)):
        date = dates[i]
        cur.execute("SELECT 1 FROM weather_daily WHERE date = ?", (date,))
        if cur.fetchone():
            continue

        cur.execute('''
            INSERT INTO weather_daily (
                date, temperature_max, temperature_min, wind_gust_max,
                wind_speed_max, apparent_temp_max, apparent_temp_min,
                precipitation_sum, precipitation_hours, precipitation_probability_max
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            date,
            data["daily"]["temperature_2m_max"][i],
            data["daily"]["temperature_2m_min"][i],
            data["daily"]["wind_gusts_10m_max"][i],
            data["daily"]["wind_speed_10m_max"][i],
            data["daily"]["apparent_temperature_max"][i],
            data["daily"]["apparent_temperature_min"][i],
            data["daily"]["precipitation_sum"][i],
            data["daily"]["precipitation_hours"][i],
            data["daily"]["precipitation_probability_max"][i]
        ))

        weather_inserted += 1

    print(f"✅ Inserted {weather_inserted} new daily weather rows from {start_date} to {end_date}")

except Exception as e:
    print("❌ Error fetching weather data:", e)



# --- HOLIDAY + DAILY WEATHER MATCHING TABLE ---
cur.execute('''
    CREATE TABLE IF NOT EXISTS holiday_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        holiday_id INTEGER,
        weather_date TEXT,
        FOREIGN KEY (holiday_id) REFERENCES holidays(id),
        FOREIGN KEY (weather_date) REFERENCES weather_daily(date),
        UNIQUE(holiday_id, weather_date)
    )
''')

cur.execute("SELECT id, date FROM holidays")
holidays = cur.fetchall()

matched = 0
max_links = 25

for holiday_id, holiday_date in holidays:
    if matched >= max_links:
        break

    # Find daily weather with exact matching date
    cur.execute("SELECT date FROM weather_daily WHERE date = ? LIMIT 1", (holiday_date,))
    result = cur.fetchone()

    if result:
        weather_date = result[0]
        cur.execute('''
            INSERT OR IGNORE INTO holiday_weather (holiday_id, weather_date)
            VALUES (?, ?)
        ''', (holiday_id, weather_date))
        matched += 1

print(f"🔗 Linked {matched} holidays to daily weather rows")



# FINALIZE
conn.commit()
conn.close()
print("🎉 All data saved to holidays.db")
