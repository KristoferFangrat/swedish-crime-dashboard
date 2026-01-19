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
    rows_skipped = 0
    
    for event in events:
        try:
            # Extract fields
            event_id = str(event.get("id", ""))
            name = str(event.get("name", ""))
            description = str(event.get("summary", ""))  # API uses 'summary' not 'description'
            event_type = str(event.get("type", ""))
            
            # Parse location - API returns location as dict with 'name' and 'gps'
            location_data = event.get("location", {})
            location_name = location_data.get("name", "") if isinstance(location_data, dict) else str(location_data)
            location = json.dumps(location_data) if isinstance(location_data, dict) else str(location_data)
            
            # Parse GPS coordinates from "latitude,longitude" format
            latitude = None
            longitude = None
            gps_string = location_data.get("gps", "") if isinstance(location_data, dict) else ""
            
            if gps_string:
                try:
                    lat_str, lon_str = gps_string.strip().split(",")
                    latitude = float(lat_str.strip())
                    longitude = float(lon_str.strip())
                except (ValueError, AttributeError) as e:
                    print(f"Warning: Could not parse GPS '{gps_string}' for event {event_id}: {e}")
            
            datetime_val = str(event.get("datetime", ""))
            affected_area = location_name  # Use location name as affected area
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
            
            if rows_inserted % 50 == 0:
                print(f"Inserted {rows_inserted} events so far...")
            
        except Exception as e:
            rows_skipped += 1
            print(f"Error inserting event {event.get('id', 'unknown')}: {e}")
    
    conn.commit()
    cursor.close()
    print(f"Inserted {rows_inserted} events, skipped {rows_skipped} events")

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
        
        # Truncate staging table to avoid duplicates
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE crime_db.PUBLIC.police_events_staging")
        cursor.close()
        print("Cleared staging table")
        
        # Insert events
        insert_events(conn, events)
        
        # Verify load and show statistics
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total_events,
                COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as events_with_coords,
                COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as events_without_coords
            FROM crime_db.PUBLIC.police_events_staging
        """)
        result = cursor.fetchone()
        cursor.close()
        
        total, with_coords, without_coords = result
        print(f"\nLoad Statistics:")
        print(f"  Total events: {total}")
        print(f"  Events with coordinates: {with_coords}")
        print(f"  Events without coordinates: {without_coords}")
        
        if total > 0:
            coverage = (with_coords / total) * 100
            print(f"  Coverage: {coverage:.1f}%")
        
        conn.close()
        print("\nData load complete!")
        
    except snowflake.connector.errors.Error as e:
        print(f"Snowflake error: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
