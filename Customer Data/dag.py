from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.email_operator import EmailOperator 

# Import your script
from main_script import create_or_append_google_sheets_table

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
 'owner': 'airflow',
 'depends_on_past': False,
 'start_date': datetime(2023, 11, 1),
 'email_on_failure': True,
 'email_on_retry': False,
 'retries': 1,
 'retry_delay': timedelta(minutes=5),
 'schedule_interval':  '0 5 * * *',  # Schedule daily at 5 AM UTC
}

# Define the DAG
dag = DAG(
    'my_daily_report_dag',
    default_args=default_args,
    description='A DAG to run the daily report script',
)

# Define tasks

# Dummy start task
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# PythonOperator to execute the main script
execute_script_task = PythonOperator(
    task_id='execute_script',
    python_callable=create_or_append_google_sheets_table,
    dag=dag,
)

# EmailOperator to send email on failure
email_on_failure_task = EmailOperator(
    task_id='email_on_failure',
    to='your@email.com',  # Replace with your email address
    subject='Airflow - Daily Report DAG Failed',
    html_content='The Daily Report DAG has failed. Please check the Airflow logs for more information.',
    dag=dag,
)

# EmailOperator to send email on success
email_on_success_task = EmailOperator(
    task_id='email_on_success',
    to='your@email.com',  # Replace with your email address
    subject='Airflow - Daily Report DAG Succeeded',
    html_content='The Daily Report DAG has succeeded.',
    dag=dag,
)

# Dummy end task
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start_task >> execute_script_task
execute_script_task >> email_on_failure_task
execute_script_task >> email_on_success_task
execute_script_task >> end_task
