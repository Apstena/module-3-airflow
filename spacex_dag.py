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
            task_id="get_data_" + rocket + "_" + str(execution_date['year']),    
            bash_command = "python3 /root/airflow/dags/spacex/load_launches.py -y {0} -o /root/airflow/data -r {1}".format(execution_date['year'], rocket if rocket !=all else ''),
            dag=dag
        )

        t2 = BashOperator(
            task_id="print_data_" + rocket + "_" + str(execution_date['year']),
            bash_command = "cat /root/airflow/data/year={0}/rocket={1}/data.csv".format(execution_date['year'], rocket),
            dag=dag
        )
        
        t1 >> t2
    execution_date['year'] += 1
