
# 1 Import modules for DAG

# from asyncio import tasks
from numpy import extract
from airflow.modules import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import requests
import json


API = "https://github.com/mathdroid/covid-19-api"
temp_api = "https://gorest.co.in/public/v2/users"

# 2 SET default arguments
default_args = {
    'owner': 'airflow'
    'depends_on_past': False,
    'email': ['airflow@example.com','ganesh@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#6 define tasks related functions
    def extract(**kwargs):
        ti =kwargs['ti']  #task_instance = ti
        response_API = requests.get(API)
        #print(response_API.status_code)
        data = response_API.text
        new_data = json.loads(data)
        ti.xcom_push('api_data',new_data)


    def removeduplicate(**kwargs):
        ti =kwargs['ti']
        duplicate_data = ti.xcom_pull(task_ids = 'extract',key = 'api_data')
        
        seen = []
        for x in duplicate_data:
            if x not in seen:
                yield x
                seen.append(x)
        r_data = removeduplicate()
        ti.xcom_push('distinct_data', r_data)


    def transform(**kwargs):
        ti =kwargs['ti'] 
        extracted_data = ti.xcom_pull(task_ids = 'removeduplicate',key = 'distinct_data')
        
        total_order_value = 0
        for dict in extracted_data:
            to_load = dict
        ti.xcom_push('total_data', to_load)
    
    def load(**kwargs):
        ti = kwargs['ti']
        load_data =ti.xcom_pull(tasks_ids = 'transform' , key = 'total_data')
        print(load_data)


# 3 Intial DAG

with DAG(
    'ETL_dag',  # dag_id
    description='This is my first DAG ',
    default_args=default_args,
    schedule_intreval=timedelta(days=1),
    start_time=datetime(2022-5-8),
    catchup=False,
    tags=['ETL'],
) as dag:

# 4 perform Tasks
    

    #task1

    extract_task.doc_md = dedent( """\#### Extract task
    A simple Extract task to get data ready for the rest api of the data pipeline.
    In this case, getting data is simulated by reading from a rest api in form of JSON string.
    This data is then put into xcom, so that it can be processed by the next task.
    """ )

    extract_data = PythonOperator(
        task_id='extract',
        python_collable=extract,
        provide_context =True
    )
    

    #task2

    remove_task.doc_md = dedent("""\
    #### Remove task
    A simple Remove task which takes data from xcom and remove redundent values.
    This value is then put into xcom, so that it can be processed by the next task.
    """ )  

    removeduplicate_data = PythonOperator(
        task_id='removeduplicate',
        python_collable=removeduplicate,
        provide_context =True
    )
    
    #task3

    transform_task.doc_md = dedent("""\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total data.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """ )  

     transform_data = PythonOperator(
        task_id='transform',
        python_collable=transform,
        provide_context =True
    )
    
    #task4

    load_task.doc_md = dedent("""\
    #### Load task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """ )

     load_data = PythonOperator(
        task_id='load',
        python_collable=load,
        provide_context =True
    )

#set_dependencies

extract_data >> removeduplicate_data >> transform_data >> load_data