### Names: Maason Reiter and Noah Chang
### Final_Project for SI206 
### Emails: mwreiter@umich.edu noahchan@umich.edu
### Notes: Pulling game prices from Nexarda API using known game IDs

import sqlite3
import requests
import time

# Database setup
conn = sqlite3.connect('games.db')  # This will create a file 'games.db' in the current directory
cursor = conn.cursor()

# Create the table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS game_ids (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL UNIQUE
)
''')
conn.commit()

headers = {
    "User-Agent": "Mozilla/5.0"
}

valid_ids = []
id_start = 1  # starting from 1
id_end = 5000  # scan up to 5000
target_valid_count = 100

for game_id in range(id_start, id_end):
    url = f"https://www.nexarda.com/api/v3/prices?type=game&id={game_id}&currency=GBP"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        json_data = response.json()
        if json_data.get("success") is True:
            valid_ids.append(game_id)
            print(f"✅ Found valid ID: {game_id} - {json_data.get('message')}")
            
            # Insert valid game_id into the database
            cursor.execute("INSERT OR IGNORE INTO game_ids (game_id) VALUES (?)", (game_id,))
            conn.commit()
        
        # Stop once we have 100 valid IDs
        if len(valid_ids) >= target_valid_count:
            break
    else:
        print(f"❌ ID {game_id} - Status {response.status_code}")

    time.sleep(0.1)  # be nice to the server

# ✅ Save or use valid_ids
print(f"\n🎉 Collected {len(valid_ids)} valid game IDs:\n{valid_ids}")

# Closing the database connection
conn.close()
