from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='Amadeus_Main',
    default_args=args,
    schedule_interval='*/10 * * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['my_amadeus_final_project']
)

run_this_amadeus = BashOperator(
    task_id='run_this_amadeus',
    bash_command='python /tmp/pycharm_project_614/MY_Amadeus_Final/amadeus_main.py',
    dag=dag,
)
run_this_kafka = BashOperator(
    task_id='run_this_kafka',
    bash_command='python /tmp/pycharm_project_614/MY_Amadeus_Final/kafka_to_spark.py',
    dag=dag,
)
dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

dummy_operator >> run_this_amadeus
dummy_operator >> run_this_kafka


if __name__ == "__main__":
    dag.cli()