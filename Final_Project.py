### Names: Mason Reiter Noah Chang
### Email: mwreiter@umich.edu noahchan@umich.edu
### Notes: Calendar Holiday Umich Comparisson Project

import sqlite3
import requests

# Setup API
API_KEY = "2Cmq6y302J2wSAtLtdGBxYnkZoxDW4aV"
base_url = "https://calendarific.com/api/v2/holidays"
country = "US"
year = 2025

# Connect to SQLite
conn = sqlite3.connect("holidays.db")
cur = conn.cursor()

# Create table if not exists
cur.execute('''
    CREATE TABLE IF NOT EXISTS holidays (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        description TEXT,
        date TEXT UNIQUE,  -- Prevents duplicates by date
        month INTEGER
    )
''')

# Track how many new rows we insert this run
inserted_count = 0
max_insert = 25

# Go month by month
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

                # Skip duplicates in same run
                key = (name.lower(), date)
                if key in seen:
                    continue
                seen.add(key)

                # Skip if 'bank' in name or description
                if "bank" in name.lower() or "bank" in description.lower():
                    continue

                # Check if this date already exists in DB
                cur.execute("SELECT 1 FROM holidays WHERE date = ?", (date,))
                if cur.fetchone():
                    continue  # Already exists

                # Insert new record
                cur.execute('''
                    INSERT INTO holidays (name, description, date, month)
                    VALUES (?, ?, ?, ?)
                ''', (name, description, date, month))
                inserted_count += 1

        else:
            print(f"No data for month {month}.")

    except Exception as e:
        print(f"Error for month {month}: {e}")

# Save & close
conn.commit()
conn.close()

print(f"✅ Inserted {inserted_count} new holiday(s) into holidays.db")


def merge_databses(path1, path2):
    #Function to merge the second API database once it is made. 
    # Connect to source and target databases
    source_conn = sqlite3.connect(source_db_path)
    target_conn = sqlite3.connect(target_db_path)

    source_cur = source_conn.cursor()
    target_cur = target_conn.cursor()

    # Optional: Create the target holidays table if it doesn't exist
    target_cur.execute('''
        CREATE TABLE IF NOT EXISTS holidays (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            description TEXT,
            date TEXT UNIQUE,
            month INTEGER
        )
    ''')