from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'alevanov'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_data_lake_etl',
    default_args = default_args,
    description = 'Data Lake ETL tasks',
    schedule_interval = "0 0 1 1 *",
)

tables = {'ods_billing': ['user_id, billing_period, service, tariff, CAST(sum as INT), CAST(created_at as DATE)',
                          'stg_billing', 'created_at'],
          'ods_issue': [
              'CAST(user_id as INT), CAST(start_time as DATE), CAST(end_time as DATE), title, description, service',
              'stg_issue', 'start_time'],
          'ods_payment': [
              'user_id, pay_doc_type, CAST(pay_doc_num as INT), account, phone, billing_period, CAST(pay_date as DATE), CAST(sum as INT)',
              'stg_payment', 'pay_date'],
          'ods_traffic': [
              'user_id, CAST(CAST(`timestamp` as BIGINT) as TIMESTAMP), device_id, device_ip_addr, CAST(bytes_sent as INT), CAST(bytes_received as INT)',
              'stg_traffic', 'CAST(CAST(`timestamp` as BIGINT) as TIMESTAMP)'],
          'dm_traffic' : ['user_id, MAX(bytes_received), MIN(bytes_received), AVG(bytes_received)', 'ods_traffic', 'year']}
params = {'current_year' : 2012, 'job_suffix': randint(0, 100000)}

dt = datetime.today()

while params['current_year'] <= dt.year:
    for i in tables:
        if i != 'dm_traffic':
            data_proc = DataProcHiveOperator(
                task_id = i,
                dag = dag,
                query = """INSERT OVERWRITE TABLE alevanov.{3} PARTITION (year={4})
                SELECT {0} FROM alevanov.{1} WHERE year({2}) = {4};""".format(tables[i][0], tables[i][1],
                                                                                                    tables[i][2], i, params['current_year']),
                cluster_name = 'cluster-dataproc',
                job_name = USERNAME + '_{0}_{1}_{2}'.format(i, params['current_year'], params['job_suffix']),
                region = 'europe-west3'
            )
            if i == 'ods_traffic':
                date_proc_dm = DataProcHiveOperator(
                    task_id='dm_traffic',
                    dag=dag,
                    query="""INSERT OVERWRITE TABLE alevanov.{3} PARTITION (year={4})
                            SELECT {0} FROM alevanov.{1} WHERE year({2}) = {4} GROUP BY user_id;""".format(tables['dm_traffic'][0], tables['dm_traffic'][1],
                                                                                          tables['dm_traffic'][2], 'dm_traffic',
                                                                                          params['current_year']),
                    cluster_name='cluster-dataproc',
                    job_name=USERNAME + '_{0}_{1}_{2}'.format('dm_traffic', params['current_year'], params['job_suffix']),
                    region='europe-west3'
                )
                data_proc >> date_proc_dm
        params['current_year'] += 1
