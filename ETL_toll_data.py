from airflow import DAG
import datetime as dt
from datetime import timedelta
from airflow.operator.bash_operator import BashOperator
from airflow.uitls.dates import days_ago

# task1.1 define dag dag arguments
default_args = {
    'owner': 'Aayush',
    'start_date': days_ago(0),
    'email': ['aayu@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

# task1.2 define dag
dag = DAG(
    'ETL_toll_data',
    description='Apache Airflow Final Assignment',
    default_args=default_args,
    schedule_interval=dt.timedelta(days=1)
)

# task1.3 unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag
)

# task1.4 extract data from csv file
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 vehicle_data.csv',
    dag=dag,
)


# task1.5 extract data from tsv file
extract_data_from_tsv_task = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d"," -f5-7 tollplaza-data.tsv > tsv_data.csv',
    dag=dag,
)

# Task 1.6 - Create a task to extract data from fixed width file
extract_data_from_fixed_width_task = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c59-68 payment-data.txt | tr " " "," > fixed_width_data.csv',
    dag=dag,
)

# Task 1.7 - Create a task to consolidate data extracted from previous tasks
consolidate_data_task = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

# Task 1.8 - Transform and load the data
transform_data_task = BashOperator(
    task_id='transform_data',
    bash_command='awk $5=toupper($5) < extracted_data.csv > transform_data.csv',
    dag=dag,
)

# Define task dependencies
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
