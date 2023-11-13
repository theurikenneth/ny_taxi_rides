from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 

def find_next_monday(start_date):
    days_ahead = 0 - start_date.weekly() + 1 # Monday is 0, so we add 1
    next_monday = start_date + timedelta(days = days_ahead)
    return next_monday

# Adjust the start_date if it doesn't fall on a Monday
specified_start_date = datetime(2023, 5, 1)
adjusted_start_date = find_next_monday(specified_start_date)

default_args = {
    'owner': 'kenneth',
    'start_date': adjusted_start_date,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'taxi_data_dag',
    default_args = default_args,
    schedule_interval = timedelta(weeks = 1),
)

def refresh_function():
    # Add your custom code or function logic here
    # This function will be executed as part of the DAG
    
    # Example code:
    print("Performing weekly refresh...")
    # Add your specific logic here

weekly_refresh_task = PythonOperator(task_id='weekly_refresh_task', python_callable=refresh_function, dag=dag)
   