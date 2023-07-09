from airflow.providers.sqlite.operators.sqlite import SqliteOperator
import xmltodict
import requests
import pendulum
from airflow.decorators import dag, task
import sys
sys.path.append("/path/to/py_env/lib")


@dag(
    dag_id='podcast_summary',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 7, 8),
    catchup=False
)
def podcast_summary():
    create_database = SqliteOperator(
        task_id='create_table_sqlite',
        sql=r"""
        CREATE TABLE IF NOT EXISTS episodes (
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT,
            transcript TEXT
        );
        """
    )

    @task()
    def get_episode():
        data = requests.get(
            "https://www.marketplace.org/feed/podcast/marketplace/")
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes.")
        return episodes

    podcast_episodes = get_episode()
    create_database.set_downstream(podcast_episodes)


summary = podcast_summary()
