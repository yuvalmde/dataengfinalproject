from builtins import range
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
    dag_id='Amadeus_Daily_Archive',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['my_final_project']
)

run_this = BashOperator(
    task_id='run_after_loop',
    bash_command='python /tmp/pycharm_project_614/MY_Amadeus_Final/daily_archive.py',
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()