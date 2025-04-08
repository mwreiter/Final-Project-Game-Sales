### Names: Maason Reiter and Noah Chang
### Final_Project for SI206 
### Emails: mwreiter@umich.edu noahchan@umich.edu
### Notes: Calendar and Holidays Project

import sqlite3
import requests
import time

### Names: Maason Reiter and Noah Chang
### Final_Project for SI206
### Emails: mwreiter@umich.edu
### Notes: Calendar Holiday Project Idea


import requests


API_KEY = "2Cmq6y302J2wSAtLtdGBxYnkZoxDW4aV"
base_url = "https://calendarific.com/api/v2/holidays"


country = "US"
year = 2025


#print(f"Holidays for {country} in {year} (max 10 per month, excluding bank holidays):\n")


for month in range(1, 13):
   params = {
       "api_key": API_KEY,
       "country": country,
       "year": year,
       "month": month,
   }

#Code to print info
   try:
       response = requests.get(base_url, params=params)
       data = response.json()


       if "response" in data and "holidays" in data["response"]:
           holidays = data["response"]["holidays"]


           print(f"\n📅 Month: {month:02}")
           count = 0
           seen = set()


           for holiday in holidays:
               name = holiday.get("name", "").lower()
               description = holiday.get("description", "").lower()


               # Filter out "bank" holidays
               if "bank" in name or "bank" in description:
                   continue


               date = holiday['date']['iso']
               key = (name, date)


               if key not in seen:
                   seen.add(key)
                   print(f"{date} - {holiday['name']}")
                   count += 1


               if count >= 12:
                   break
       else:
           print(f"No data returned for month {month}.")


   except Exception as e:
       print(f"Error for month {month}:", e)








