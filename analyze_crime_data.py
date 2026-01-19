import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection config
snowflake_config = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": "crime_db",
    "schema": "mart",
    "role": "ACCOUNTADMIN"
}

def get_data(query):
    """Execute query and return results as DataFrame"""
    try:
        conn = snowflake.connector.connect(**snowflake_config)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# Analysis 1: Event count by type
print("📊 ANALYSIS 1: Events by Type")
print("=" * 50)
query1 = """
SELECT 
    type,
    COUNT(*) as event_count
FROM fct_police_events
GROUP BY type
ORDER BY event_count DESC
LIMIT 10
"""
df1 = get_data(query1)
if df1 is not None:
    print(df1.to_string(index=False))
    print()

# Analysis 2: Events by hour of day
print("📊 ANALYSIS 2: Events by Hour of Day")
print("=" * 50)
query2 = """
SELECT 
    event_hour,
    COUNT(*) as event_count
FROM fct_police_events
WHERE event_hour IS NOT NULL
GROUP BY event_hour
ORDER BY event_hour
"""
df2 = get_data(query2)
if df2 is not None:
    print(df2.to_string(index=False))
    print()

# Analysis 3: Events by day of week
print("📊 ANALYSIS 3: Events by Day of Week")
print("=" * 50)
day_names = {1: 'Sunday', 2: 'Monday', 3: 'Tuesday', 4: 'Wednesday', 5: 'Thursday', 6: 'Friday', 7: 'Saturday'}
query3 = """
SELECT 
    day_of_week,
    COUNT(*) as event_count
FROM fct_police_events
WHERE day_of_week IS NOT NULL
GROUP BY day_of_week
ORDER BY day_of_week
"""
df3 = get_data(query3)
if df3 is not None:
    df3['day_name'] = df3['day_of_week'].map(day_names)
    print(df3[['day_of_week', 'day_name', 'event_count']].to_string(index=False))
    print()

# Analysis 4: Top 10 locations
print("📊 ANALYSIS 4: Top 10 Locations")
print("=" * 50)
query4 = """
SELECT 
    location,
    COUNT(*) as event_count
FROM fct_police_events
WHERE location IS NOT NULL AND location != ''
GROUP BY location
ORDER BY event_count DESC
LIMIT 10
"""
df4 = get_data(query4)
if df4 is not None:
    print(df4.to_string(index=False))
    print()

# Analysis 5: Events with GPS coordinates
print("📊 ANALYSIS 5: GPS Coverage Statistics")
print("=" * 50)
query5 = """
SELECT 
    COUNT(*) as total_events,
    SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) as events_with_coordinates,
    ROUND(100.0 * SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as coverage_percent
FROM fct_police_events
"""
df5 = get_data(query5)
if df5 is not None:
    print(df5.to_string(index=False))
    print()

# Create visualizations
print("\n📈 Creating visualizations...")

fig, axes = plt.subplots(2, 2, figsize=(15, 12))

# Plot 1: Top 10 event types
if df1 is not None:
    ax = axes[0, 0]
    df1_top = df1.head(10)
    ax.barh(df1_top['type'], df1_top['event_count'], color='steelblue')
    ax.set_xlabel('Event Count')
    ax.set_title('Top 10 Event Types')
    ax.invert_yaxis()

# Plot 2: Events by hour
if df2 is not None:
    ax = axes[0, 1]
    ax.plot(df2['event_hour'], df2['event_count'], marker='o', color='coral', linewidth=2)
    ax.set_xlabel('Hour of Day')
    ax.set_ylabel('Event Count')
    ax.set_title('Events by Hour of Day')
    ax.grid(True, alpha=0.3)
    ax.set_xticks(range(0, 24, 2))

# Plot 3: Events by day of week
if df3 is not None:
    ax = axes[1, 0]
    day_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    df3_sorted = df3.set_index('day_name').loc[day_order]
    ax.bar(range(len(df3_sorted)), df3_sorted['event_count'], color='green', alpha=0.7)
    ax.set_xticks(range(len(df3_sorted)))
    ax.set_xticklabels(day_order, rotation=45)
    ax.set_ylabel('Event Count')
    ax.set_title('Events by Day of Week')

# Plot 4: Top 10 locations
if df4 is not None:
    ax = axes[1, 1]
    df4_top = df4.head(10)
    ax.barh(df4_top['location'], df4_top['event_count'], color='purple', alpha=0.7)
    ax.set_xlabel('Event Count')
    ax.set_title('Top 10 Event Locations')
    ax.invert_yaxis()

plt.tight_layout()
plt.savefig('crime_analysis.png', dpi=300, bbox_inches='tight')
print("✅ Visualization saved as 'crime_analysis.png'")

# Summary statistics
print("\n" + "=" * 50)
print("📊 SUMMARY STATISTICS")
print("=" * 50)
query_summary = "SELECT COUNT(*) as total_events, COUNT(DISTINCT type) as unique_types FROM fct_police_events"
df_summary = get_data(query_summary)
if df_summary is not None:
    print(f"Total Events: {df_summary['total_events'][0]:,}")
    print(f"Unique Event Types: {df_summary['unique_types'][0]}")
