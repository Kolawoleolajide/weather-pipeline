import requests
import psycopg2
from datetime import datetime

API_KEY = "bdf17317ccaa08f29bfabb76881e56c9"

url = f"https://api.openweathermap.org/data/2.5/weather?q=Lagos&appid=bdf17317ccaa08f29bfabb76881e56c9&units=metric"

response = requests.get(url)
data = response.json()

record = (
    datetime.now(),
    "Lagos",
    data["main"]["temp"],
    data["main"]["humidity"],
    data["wind"]["speed"],
    data["weather"][0]["description"]
)

conn = psycopg2.connect(
    "postgresql://postgres.byntkxqnjshttsyrgczo:Ball2347962%25@aws-0-eu-west-1.pooler.supabase.com:6543/postgres?sslmode=require"
)


cur = conn.cursor()

query = """
INSERT INTO weather_data (timestamp, city, temperature, humidity, windspeed, condition)
VALUES (%s, %s, %s, %s, %s, %s)
"""

cur.execute(query, record)

conn.commit()
cur.close()
conn.close()

print("Data inserted")

if __name__ == "__main__":
    print("Running pipeline...")
