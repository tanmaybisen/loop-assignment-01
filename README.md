#### Loop Backend Engineer - Assignment 01
Application generates report in 4.11 mins.

#### Technologies
FastAPI, Python, SQLAlchemy, Psycopg2, PostgreSQL(DB)

#### Summary
- The app runs on Piece-wise constant interpolation method. It is assumed that this data is dynamic and with very little or no modifications the report can be generated for hourly updating data.

- The given CSVs are stored into a PostgreSQL database locally and API call is being made to get the data.

- The report output has 7 columns for each of the store ids
store_id,
uptime_last_hour(in minutes), uptime_last_day(in hours), update_last_week(in hours),
downtime_last_hour(in minutes), downtime_last_day(in hours), downtime_last_week(in hours)

- Uptime and downtime should only includes observations within business hours. Uptime and downtime rely on "Piece-wise constant interpolation" based on the periodic polls given in store_status.

- Max timestamp among all the observations is considered as the Current timestamp. Tow endpoints are created for the API. 
    (A) /trigger_report = trigger report generation
        returns report_id (random string)
    (B) /get_report = return the status of the report or the csv
        Input: report_id, Output: “Running” / return CSV file

#### Execution steps on local machine after DB Setup
#### Create Virtual Env
python -m venv venv

#### To Run in local (env) git-bash cmd
source ./nenv/Scripts/activate

#### To Run FastAPI app on localhost
uvicorn main:app --reload

