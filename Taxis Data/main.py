import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from green_taxi import green_taxi
from yellow_taxi import yellow_taxi

# Load credentials from the downloaded JSON file
credentials = Credentials.from_service_account_file('/home/oldkent/Downloads/credentials.json')

# Authenticate with Google Sheets API
scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/spreadsheets'])
client = gspread.Client(auth = scoped_credentials)
client.session.headers.update({'Content-Type': 'application/json'})

# Get the current month and year
now = datetime.now()
date = now.strftime("%Y-%m-%d")
month_year = now.strftime("%B_%Y")

# Open the existing Google Sheet
spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1-3mw2JO3Pj-AZgX2NsDnR6qPuiV3Ig-CqFOq_vNfo-4/edit#gid=0")
worksheet = spreadsheet.sheet1

# Check if the current month matches the current date
if month_year not in spreadsheet.title:
    #Create a new Google Sheet with the current month and year in the name
    new_sheet = client.create(f"Data_Sheet_{month_year}")
    new_worksheet = new_sheet

# Function to execute queries and get data
def execute_queries_and_get_data():
    green_taxi_data = green_taxi()
    yellow_taxi_data = yellow_taxi()
    return green_taxi_data, yellow_taxi_data

# Data manipulation function
def manipulate_data(green_data, yellow_data):
    # Create DataFrames for green data and yellow data
    df1 = pd.DataFrame({'green_taxi': green_data})
    df2 = pd.DataFrame({'yellow_taxi': yellow_data})

    # Concatenate the two Dataframes
    concatenated_data = pd.concat([df1, df2], axis = 1)

    # Return the concatenated data
    return concatenated_data

# Execute queries and get data
green_taxi_data, yellow_taxi_data = execute_queries_and_get_data()

# Perform data manipulation
manipulated_data = manipulate_data(green_taxi_data, yellow_taxi_data)

# Export the manipulated data to the Google Sheet
worksheet.update([manipulated_data.columns.tolist()] + manipulated_data.values.tolist())

print("Data exported successfully to Google Sheet!")
