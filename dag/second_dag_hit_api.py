
# 1 Import modules for DAG

# from asyncio import tasks
from platform import python_branch
from numpy import extract
from sqlalchemy import DATE
from airflow import DAG
from datetime import timedelta,datetime
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
from datetime import date
from datetime import timedelta

API_SUFFIX = ".csv"
PATH = "C:\\Users\\GANESH\\Desktop\\airflowqq\\dags_check\\"
API =  "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"
today = date.today()
yesterday = today - timedelta(days=1)
DATE = yesterday.strftime("%m-%d-%Y")
# 2 SET default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com','ganesh@gmail.com'],
    "start_date": datetime(2022, 6, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#6 define tasks related functions


def extract(**kwargs):
    ti =kwargs['ti']  #task_instance = ti
    res=None
    try:
        res = requests.request('get', API+DATE+API_SUFFIX)
        
    except Exception as e:
        print(e) 
    ti.xcom_push('api_data',res)




def save(**kwargs):
    ti =kwargs['ti']
    response = ti.xcom_pull(task_ids = 'extract',key = 'api_data')
    if response != None and response.status_code == 200:
        with open(PATH+DATE+API_SUFFIX, "wb")as file: 
            for line in response.iter_lines(delimiter=b'\\n'):
                file.write(line)
                print(f"SUCCESSFULLY DOWNLOAD: {DATE+API_SUFFIX}")
    read_data_location = PATH+DATE+API_SUFFIX
    ti.xcom_push('destination_data', read_data_location)


def transform(**kwargs):
    ti =kwargs['ti'] 
    raw_data = ti.xcom_pull(task_ids = 'save',key = 'destination_data')
    covid_data= pd.read_csv(raw_data)
    covid_data['Active'] = covid_data['Confirmed'] - covid_data['Deaths'] - covid_data['Recovered']
    result = covid_data.groupby('Country_Region')['Confirmed', 'Deaths', 'Recovered', 'Active'].sum().reset_index()
    to_load = result
    ti.xcom_push('filtered_data', to_load)

def load(**kwargs):
    ti = kwargs['ti']
    load_data =ti.xcom_pull(task_ids = 'transform' , key = 'filtered_data')
    print(load_data)
    load_data.to_csv(PATH+DATE+API_SUFFIX)


# 3 Intial DAG

with DAG(
    'ETL_dag',  # dag_id
    description='This is my first DAG ',
    default_args=default_args,
    schedule_interval='@daily' ,#timedelta(minutes=2),
    catchup=False,
    tags=['ETL'],
) as dag:

# 4 perform Tasks

    #task1

    extract_data = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context =True
    )
    

    #task2 

    save_data = PythonOperator(
        task_id='save',
        python_callable=save,
        provide_context =True
    )
    
    #task3

    transform_data = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context =True
    )
    
    #task4

    load_data = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context =True
    )

#set_dependencies

extract_data >> save_data >> transform_data >> load_data