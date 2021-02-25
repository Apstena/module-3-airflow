from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("spacex", default_args=default_args, schedule_interval="0 0 1 1 *")
execution_date = {"year": 2005}
rocket_list = ['falcon1', 'falcon9', 'falconheavy', 'all', ]
while execution_date['year'] <= 2021:    
    for rocket in rocket_list:
        t1 = BashOperator(
            task_id="get_data_" + rocket + "_" + execution_date['year'],    
            bash_command = f"python3 /root/airflow/dags/spacex/load_launches.py -y {execution_date['year']} -o /root/airflow/data -r {rocket}",
            dag=dag
        )

        t2 = BashOperator(
            task_id="print_data_" + rocket + "_" + execution_date['year'],
            bash_command = f"cat /root/airflow/data/year={execution_date['year']}/rocket={rocket}/data.csv",
            dag=dag
        )
        
        t1 >> t2
    execution_date['year'] += 1
