
#1 Import modules for DAG

from airflow.modules import DAG
from datetime import timedelta,datetime
from airflow.operators.bash import BashOperator

#2 SET default arguments

 default_args={
        'owner':'airflow'
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    }

# 3 Intial DAG

with DAG(
    'first_dag',  # dag_id
    description='This is my first DAG ',
    default_args=default_args,
    schedule_intreval=timedelta(days=1),
    start_time=datetime(2022-5-8),
    catchup=False,
    tags=['first_dag'],
) as dag:

# 4 perform Tasks   

# Create tasks
    t0 = BashOperator(task_id = 'task1')
    t1 = BashOperator(task_id = 'task2')
    t2 = BashOperator(task_id = 'task3')

#5 set dependencies

t0>>t1>>t2
