#1 Import modules for DAG

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

#2 SET default arguments

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'queue': 'bash_queue',
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


with DAG("tutorial",
        default_args=default_args, 
        schedule_interval=timedelta(1)
        ) as dag:

# 4 perform Tasks   

# t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

    t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)

    templated_command = """
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7)}}"
            echo "{{ params.my_param }}"
        {% endfor %}
    """

    t3 = BashOperator(
        task_id="templated",
        bash_command=templated_command,
        params={"my_param": "Parameter I passed in"},
        dag=dag,
    )


#5 set dependencies

t2.set_upstream(t1)
t3.set_upstream(t1)

