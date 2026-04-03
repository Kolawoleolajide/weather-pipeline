import requests
import psycopg2
from datetime import datetime
import os

# --- List of cities you want to track ---
cities = ["Lagos", "Abuja", "Kano", "Port Harcourt"]

API_KEY = "bdf17317ccaa08f29bfabb76881e56c9"

# --- Connect to Supabase DB ---
conn = psycopg2.connect(os.getenv("DB_URL"))
cur = conn.cursor()

for city in cities:
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()
    
    record = (
        datetime.now(),
        city,
        data["main"]["temp"],
        data["main"]["humidity"],
        data["wind"]["speed"],
        data["weather"][0]["description"]
    )

    query = """
    INSERT INTO weather_data (timestamp, city, temperature, humidity, windspeed, condition)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur.execute(query, record)
    print(f"Inserted data for {city}")

conn.commit()
cur.close()
conn.close()

if __name__ == "__main__":
    print("Running pipeline for multiple cities...")
