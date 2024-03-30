import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="test_dag",
    start_date=datetime.datetime(2024, 3, 23),
    schedule=None
)
def main_dag():
    @task(
        task_id="test_task"
    )
    def test_task():
        print("Airflow is working correctly!")

    test_task()
main_dag()