# dbt Crime Analytics Project

DBT project for transforming and testing Swedish police event data.

## Setup

1. Install dbt and dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure your `.env` file has Snowflake credentials:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=crime_wh
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

3. Move `profiles.yml` to your dbt profiles directory:
```bash
# On Windows
mkdir %USERPROFILE%\.dbt
copy profiles.yml %USERPROFILE%\.dbt\profiles.yml

# On macOS/Linux
mkdir ~/.dbt
cp profiles.yml ~/.dbt/profiles.yml
```

## dbt Commands

```bash
# Test connection
dbt debug

# Run models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Run specific model
dbt run --select stg_police_events

# Run with full refresh
dbt run --full-refresh
```

## Project Structure

- `models/staging/` - Data cleaning and standardization
- `models/marts/` - Analytics-ready fact and dimension tables
- `tests/` - Custom data quality tests
- `models/schema.yml` - Data documentation and built-in tests
- `models/sources.yml` - Source table definitions

## Models

### Staging Layer
- `stg_police_events` - Cleaned police events with proper types

### Mart Layer
- `fct_police_events` - Fact table with date dimensions for analysis
