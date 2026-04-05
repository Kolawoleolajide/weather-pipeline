import requests
import psycopg2
from datetime import datetime
import os
import logging
import time

# --- CONFIG ---
API_KEY = "bdf17317ccaa08f29bfabb76881e56c9"
cities = ["Lagos", "Abuja", "Kano", "Port Harcourt"]
DB_URL = os.getenv("DB_URL")

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- FUNCTION: FETCH DATA WITH RETRIES ---
def fetch_weather(city, retries=3):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.warning(f"{city} attempt {attempt+1} failed: {e}")
            time.sleep(2)
    
    raise Exception(f"Failed to fetch data for {city} after {retries} retries")

# --- MAIN PIPELINE ---
def run_pipeline():
    conn = None
    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()

        for city in cities:
            data = fetch_weather(city)

            record = (
                datetime.now(),
                city,
                data["main"]["temp"],
                data["main"]["humidity"],
                data["wind"]["speed"],
                data["weather"][0]["description"]
            )

            cur.execute("""
                INSERT INTO weather_data (timestamp, city, temperature, humidity, windspeed, condition)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, record)

            logging.info(f"Inserted data for {city}")

        # --- AUDIT SUCCESS ---
        cur.execute("""
            INSERT INTO pipeline_audit (run_time, status, message)
            VALUES (%s, %s, %s)
        """, (datetime.now(), "SUCCESS", "Pipeline ran successfully"))

        conn.commit()
        logging.info("Pipeline completed successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

        if conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO pipeline_audit (run_time, status, message)
                VALUES (%s, %s, %s)
            """, (datetime.now(), "FAILED", str(e)))
            conn.commit()

        raise  # This makes GitHub Actions show failure 🚨

    finally:
        if conn:
            conn.close()

# --- ENTRY POINT ---
if __name__ == "__main__":
    logging.info("Starting pipeline...")
    run_pipeline()
