from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2005, 1, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("spacex", default_args=default_args, schedule_interval="0 0 1 1 *")

rocket_list = ['falcon1', 'falcon9', 'falconheavy', 'all', ]
for rocket in rocket_list:
    params = {"rocket": rocket}
    execution_date = {"year": 2005}
    while execution_date['year'] <= 2021:
        t1 = BashOperator(
            task_id="get_data_" + rocket,
            #params={"rocket": rocket}
            bash_command=bash_command = "python3 /root/airflow/dags/spacex/load_launches.py -y {execution_date} -o /var/data/year={execution_date}/rocket={rocket} -r {rocket}".format(execution_date = execution_date['year'], rocket = rocket),
            dag=dag
        )

        t2 = BashOperator(
            task_id="print_data_" + rocket,
            bash_command="cat /var/data/year={execution_date}/rocket={rocket}/data.csv".format(execution_date = execution_date['year'], rocket = rocket),
            #params={"rocket": rocket},  # falcon1/falcon9/falconheavy
            dag=dag
        )
        
        t1 >> t2
        execution_date['year'] += 1
