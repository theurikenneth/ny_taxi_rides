import pandas as pd
from google.cloud import bigquery
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sql_queries import generate_sql_query
from datetime import datetime


# Function to execute SQL query in BigQuery and return results as a DataFrame
def execute_bigquery_query(sql_query):
 # Set up Bigquery client
 client = bigquery.Client()

 # Execute the SQL query and fetch the result
 query_job = client.query(sql_query)
 result = query_job.result()

 # Convert the result to a DataFrame
 result_df = result.to_dataframe()

 return result_df 

# Function to create or append to a Google Sheets table
def create_or_append_google_sheets_table(dataframe, sheet_name):
 # Set up credentials and authenticate with Google Sheets API
 scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
 credentials = ServiceAccountCredentials.from_json_keyfile_name('path/to.credentials.json', scope)
 gc = gspread.authorize(credentials)

 # Try to open the worksheet with the specified sheet name
 try:
  worksheet = gc.open(sheet_name).sheet1
 except gspread.exceptions.SpreadsheetNotFound:
  # If the sheet does not exist, create a new one
  worksheet = gc.create(sheet_name).sheet1

 # Append the DataFrame data to the worksheet
 worksheet.append_table(dataframe.values.tolist(), start='A1', end = None, dimensions = 'ROWS', overwrite = False)

# Execute the SQL query in Bigquery
sql_query = generate_sql_query()
result_df = execute_bigquery_query(sql_query)

# Generate a sheet name based on the current date
current_date = datetime.now().strftime('%Y%m%d_%H%M%S')
sheet_name = f"Data_{current_date}"

# Create or append to Google Sheets table with the dynamically generated sheet name
create_or_append_google_sheets_table(result_df, sheet_name)

print(f"Table created or appended in Google Sheets with the name: {sheet_name}")