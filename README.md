# 🚨 Swedish Crime Analysis Dashboard

This project is an interactive **Streamlit dashboard** that analyzes Swedish police event data from the **Polisen API**.  
The goal is to explore crime patterns by **type, time, and location** using SQL, Python, and Snowflake.

The dashboard is designed as part of a **Data Engineering & Data Visualization** learning project.

---

## 📊 Features

- Summary statistics (total events, unique types, locations, days covered)
- Top crime types visualization
- Events by hour of the day
- Events by day of the week
- Top event locations (cities)
- GPS coverage statistics
- Raw data explorer

---

## 🛠️ Tech Stack

- **Python** – Data processing & app logic  
- **Streamlit** – Web dashboard  
- **Snowflake** – Cloud data warehouse  
- **SQL** – Data querying  
- **Pandas** – Data handling  
- **Plotly** – Interactive visualizations  
- **dotenv** – Environment variable management  

---

## 🗄️ Data Source

The data comes from the **Swedish Police (Polisen) API**, which provides public information about police events across Sweden.

---

## ▶️ How to Run the App

1. Install dependencies:

```bash
pip install -r requirements.txt

2. Create a .env file with your Snowflake credentials:
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse

3. Run the app:
streamlit run streamlit_app.py

4. Open in browser
