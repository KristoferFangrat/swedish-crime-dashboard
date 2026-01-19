import requests
import json
import snowflake.connector
from datetime import datetime
from typing import List, Dict
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_URL = "https://polisen.se/api/events"
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE")
}

def fetch_police_events() -> List[Dict]:
    """Fetch events from Swedish police API"""
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        events = response.json()
        print(f"Fetched {len(events)} events from API")
        return events
    except requests.exceptions.RequestException as e:
        print(f"Error fetching API: {e}")
        return []

def create_staging_table(conn):
    """Create staging table if it doesn't exist"""
    sql = """
    CREATE TABLE IF NOT EXISTS crime_db.PUBLIC.police_events_staging (
        event_id STRING,
        name STRING,
        description STRING,
        type STRING,
        location STRING,
        latitude FLOAT,
        longitude FLOAT,
        datetime TIMESTAMP_NTZ,
        affected_area STRING,
        api_response VARIANT,
        loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()
    print("Staging table ready")

def insert_events(conn, events: List[Dict]):
    """Insert fetched events into staging table"""
    if not events:
        print("No events to insert")
        return
    
    cursor = conn.cursor()
    rows_inserted = 0
    
    for event in events:
        try:
            # Extract fields
            event_id = str(event.get("id", ""))
            name = str(event.get("name", ""))
            description = str(event.get("description", ""))
            event_type = str(event.get("type", ""))
            location = str(event.get("location", ""))
            latitude = event.get("latitude")
            longitude = event.get("longitude")
            datetime_val = str(event.get("datetime", ""))
            affected_area = str(event.get("affected_area", ""))
            api_response = json.dumps(event)
            
            # Use parameterized query with TRY_PARSE_JSON
            insert_sql = """
            INSERT INTO crime_db.PUBLIC.police_events_staging 
            (event_id, name, description, type, location, latitude, longitude, datetime, affected_area, api_response)
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, TRY_PARSE_JSON(%s)
            """
            
            cursor.execute(insert_sql, (
                event_id, name, description, event_type, location,
                latitude, longitude, datetime_val, affected_area, api_response
            ))
            rows_inserted += 1
            
        except Exception as e:
            if rows_inserted % 50 == 0:  # Log every 50 events
                print(f"Inserted {rows_inserted} events so far...")
    
    conn.commit()
    cursor.close()
    print(f"Inserted {rows_inserted} events into staging table")

def main():
    """Main ETL pipeline"""
    print("Starting police events data load...")
    
    # Fetch API data
    events = fetch_police_events()
    if not events:
        print("No data to load")
        return
    
    # Connect to Snowflake
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        print("Connected to Snowflake")
        
        # Create staging table
        create_staging_table(conn)
        
        # Insert events
        insert_events(conn, events)
        
        # Verify load
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM crime_db.PUBLIC.police_events_staging")
        count = cursor.fetchone()[0]
        cursor.close()
        print(f"Total events in staging: {count}")
        
        conn.close()
        print("Data load complete!")
        
    except snowflake.connector.errors.Error as e:
        print(f"Snowflake error: {e}")

if __name__ == "__main__":
    main()
