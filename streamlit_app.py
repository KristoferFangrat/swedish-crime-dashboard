# -*- coding: utf-8 -*-
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="Swedish Crime Analysis",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflake config
snowflake_config = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": "crime_db",
    "schema": "staging_mart",
    "role": "ACCOUNTADMIN"
}

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(**snowflake_config)

@st.cache_data(ttl=3600)
def get_data(query):
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Database Error: {e}")
        return None


# Title
st.title("🚨 Swedish Police Events Analysis")

st.markdown("""
This dashboard provides interactive analysis of Swedish police events data from the Polisen API.
Explore crime trends, patterns by time and location, and overall statistics.
""")

# Apply dark theme by default
st.markdown("""
<style>
/* Dark theme CSS - selective styling */
body {
    background-color: #1a1a1a !important;
    color: #ffffff !important;
}

[data-testid="stAppViewContainer"] {
    background-color: #1a1a1a !important;
}

[data-testid="stSidebar"] {
    background-color: #1a1a1a !important;
}

.stMarkdown p, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown h4, .stMarkdown h5, .stMarkdown h6 {
    color: #ffffff !important;
}

[data-testid="stMetric"] {
    background-color: #2a2a2a !important;
    color: #ffffff !important;
    border-radius: 10px;
    padding: 10px;
}

[data-testid="stDataFrame"] {
    background-color: #2a2a2a !important;
    color: #ffffff !important;
}

table {
    background-color: #2a2a2a !important;
    color: #ffffff !important;
}

th, td {
    background-color: #2a2a2a !important;
    color: #ffffff !important;
    border-color: #444444 !important;
}

.stSelectbox, .stCheckbox, .stRadio {
    color: #ffffff !important;
}

input, select, textarea {
    background-color: #2a2a2a !important;
    color: #ffffff !important;
    border-color: #444444 !important;
}

[data-testid="stInfo"] {
    background-color: #2a2a2a !important;
    color: #ffffff !important;
}

button {
    background-color: #FF0000 !important;
    color: #ffffff !important;
}

button:hover {
    background-color: #cc0000 !important;
}

hr {
    border-color: #444444 !important;
}

/* Scrollbar */
::-webkit-scrollbar-thumb {
    background-color: #444444 !important;
}
</style>
""", unsafe_allow_html=True)


# Sidebar
st.sidebar.header("📊 Dashboard Controls")
st.sidebar.button("🔄 Refresh Data")


# =======================
# Summary Statistics
# =======================
st.header("📈 Summary Statistics")

query_summary = """
SELECT 
    COUNT(*) as total_events,
    COUNT(DISTINCT type) as unique_types,
    COUNT(DISTINCT location) as unique_locations,
    COUNT(DISTINCT DATE(event_datetime)) as days_covered
FROM crime_db.staging_mart.fct_police_events
"""

df_summary = get_data(query_summary)

if df_summary is not None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Events", f"{df_summary['TOTAL_EVENTS'][0]:,}")
    col2.metric("Unique Event Types", df_summary["UNIQUE_TYPES"][0])
    col3.metric("Unique Locations", df_summary["UNIQUE_LOCATIONS"][0])
    col4.metric("Days Covered", df_summary["DAYS_COVERED"][0])


# =======================
# Top Event Types
# =======================
st.header("1️⃣ Top Event Types")

query1 = """
SELECT 
    type,
    COUNT(*) as event_count
FROM crime_db.staging_mart.fct_police_events
GROUP BY type
ORDER BY event_count DESC
LIMIT 15
"""

df1 = get_data(query1)

if df1 is not None:
    fig1 = px.bar(
        df1,
        x="EVENT_COUNT",
        y="TYPE",
        orientation="h",
        labels={"EVENT_COUNT": "Number of Events", "TYPE": "Event Type"},
        color="EVENT_COUNT",
        color_continuous_scale="Blues"
    )
    fig1.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig1, width="stretch")


# =======================
# Events by Hour
# =======================
st.header("2️⃣ Events by Hour of Day")

query2 = """
SELECT 
    event_hour,
    COUNT(*) as event_count
FROM crime_db.staging_mart.fct_police_events
WHERE event_hour IS NOT NULL
GROUP BY event_hour
ORDER BY event_hour
"""

df2 = get_data(query2)

if df2 is not None:
    fig2 = px.line(
        df2,
        x="EVENT_HOUR",
        y="EVENT_COUNT",
        markers=True,
        labels={"EVENT_HOUR": "Hour of Day", "EVENT_COUNT": "Number of Events"}
    )
    fig2.update_layout(height=350)
    st.plotly_chart(fig2, width="stretch")


# =======================
# Events by Day
# =======================
st.header("3️⃣ Events by Day of Week")

day_names = {
    1: "Sunday", 2: "Monday", 3: "Tuesday",
    4: "Wednesday", 5: "Thursday",
    6: "Friday", 7: "Saturday"
}

query3 = """
SELECT 
    day_of_week,
    COUNT(*) as event_count
FROM crime_db.staging_mart.fct_police_events
WHERE day_of_week IS NOT NULL
GROUP BY day_of_week
ORDER BY day_of_week
"""

df3 = get_data(query3)

if df3 is not None:
    df3["day_name"] = df3["DAY_OF_WEEK"].map(day_names)

    fig3 = px.bar(
        df3,
        x="day_name",
        y="EVENT_COUNT",
        labels={"day_name": "Day", "EVENT_COUNT": "Incidents"},
        color="EVENT_COUNT",
        color_continuous_scale="Greens"
    )

    fig3.update_layout(height=350, showlegend=False)
    st.plotly_chart(fig3, width="stretch")


# =======================
# Top Locations
# =======================
st.header("4️⃣ Top Event Locations")

query4 = """
SELECT 
    location,
    COUNT(*) as event_count
FROM crime_db.staging_mart.fct_police_events
WHERE location IS NOT NULL AND location != ''
GROUP BY location
ORDER BY event_count DESC
LIMIT 15
"""

df4 = get_data(query4)

if df4 is not None:
    import json
    
    def extract_location_name(location):
        if isinstance(location, dict):
            return location.get('name', 'Unknown')
        elif isinstance(location, str):
            try:
                loc_dict = json.loads(location)
                return loc_dict.get('name', 'Unknown')
            except:
                return 'Unknown'
        return 'Unknown'
    
    df4["location_name"] = df4["LOCATION"].apply(extract_location_name)

    fig4 = px.bar(
        df4,
        x="EVENT_COUNT",
        y="location_name",
        orientation="h",
        labels={"EVENT_COUNT": "Incidents", "location_name": "City"},
        color="EVENT_COUNT",
        color_continuous_scale="Purples"
    )

    fig4.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig4, use_container_width=True)


# =======================
# GPS Coverage
# =======================
st.header("5️⃣ GPS Coverage Statistics")

query5 = """
SELECT 
    COUNT(*) as total_events,
    SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) as events_with_coordinates,
    ROUND(100.0 * SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as coverage_percent
FROM crime_db.staging_mart.fct_police_events
"""

df5 = get_data(query5)

if df5 is not None:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Events", f"{df5['TOTAL_EVENTS'][0]:,}")
    col2.metric("Events with GPS", f"{df5['EVENTS_WITH_COORDINATES'][0]:,}")
    col3.metric("GPS Coverage", f"{df5['COVERAGE_PERCENT'][0]}%")


# =======================
# Raw Data Explorer
# =======================
st.header("📋 Raw Data Explorer")

query_raw = """
SELECT 
    event_id,
    type,
    location,
    event_datetime,
    day_of_week,
    CAST(latitude AS FLOAT) as latitude,
    CAST(longitude AS FLOAT) as longitude
FROM crime_db.staging_mart.fct_police_events
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
LIMIT 100
"""

df_raw = get_data(query_raw)

if df_raw is not None:
    # Decode Unicode escape sequences in JSON strings
    import json
    for col in df_raw.select_dtypes(include=['object']).columns:
        if col == 'LOCATION':
            # Handle JSON with escaped Unicode
            df_raw[col] = df_raw[col].apply(lambda x: json.loads(x) if isinstance(x, str) and x.startswith('{') else x)
    
    # Extract location name handling both dict and string formats
    def extract_location_name(location):
        if isinstance(location, dict):
            return location.get('name', 'Unknown')
        elif isinstance(location, str):
            try:
                loc_dict = json.loads(location)
                return loc_dict.get('name', 'Unknown')
            except:
                return 'Unknown'
        return 'Unknown'
    
    df_raw["location_name"] = df_raw["LOCATION"].apply(extract_location_name)
    df_raw["day_name"] = df_raw["DAY_OF_WEEK"].map(day_names)

    display_df = df_raw[
        ["EVENT_ID", "TYPE", "location_name", "EVENT_DATETIME", "day_name", "LATITUDE", "LONGITUDE"]
    ]

    display_df.columns = ["Event ID", "Type", "Location", "Date & Time", "Day", "latitude", "longitude"]
    
    # Ensure coordinates are numeric
    display_df["latitude"] = pd.to_numeric(display_df["latitude"], errors='coerce')
    display_df["longitude"] = pd.to_numeric(display_df["longitude"], errors='coerce')

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Interactive map with Plotly
    show_map = st.checkbox("Show interactive map", value=False)

    if show_map:
        # Filter out rows with missing coordinates
        map_df = display_df.dropna(subset=['latitude', 'longitude']).copy()
        
        if len(map_df) > 0:
            fig = px.scatter_mapbox(
                map_df,
                lat='latitude',
                lon='longitude',
                hover_data={
                    'Type': True, 
                    'Event ID': True, 
                    'latitude': ':.4f', 
                    'longitude': ':.4f',
                    'Location': False,
                    'Date & Time': False,
                    'Day': False
                },
                labels={'Type': 'Crime Type'},
                title='Crime Events Map',
                zoom=5,
                center={'lat': 60.1, 'lon': 18.6},  # Center on Sweden
                mapbox_style='open-street-map',
                color='Type',
                size_max=15,
                height=600
            )
            fig.update_layout(
                margin={"r": 0, "t": 30, "l": 0, "b": 0},
                hovermode='closest'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No coordinates available for map.")


# =======================
# Footer
# =======================
st.markdown("---")

col1, col2, col3 = st.columns(3)

with col1:
    st.caption("Data Source: Swedish Police API (Polisen)")

with col2:
    st.caption(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with col3:
    st.caption("Built with Streamlit 🎈")
