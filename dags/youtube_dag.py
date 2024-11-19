import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import the provided functions
sys.path.append('/opt/airflow')
from etl.extract import fetch_youtube_videos, fetch_youtube_comments

# Define your YouTube Data API key
API_KEY = os.getenv('API_KEY')

# Define default arguments for the DAG
default_args = {
    'owner': 'test',
    'depends_on_past': False,
}

# Initialize the DAG
with DAG(
    dag_id='youtube_ETL',
    default_args=default_args,
    description='DAG to extract YouTube videos and comments',
    schedule_interval=None,
    start_date=datetime(2024, 11, 16),
    catchup=False,
) as dag:

    def extract_videos(**kwargs):
        """Extract YouTube videos based on a query."""
        query = "Iphone"  # Example search query
        videos = fetch_youtube_videos(query, API_KEY, max_results=5)
        video_ids = [video['video_id'] for video in videos]
        print(f"Extracted video IDs: {video_ids}")
        # Push video_ids to XCom for the next task
        kwargs['ti'].xcom_push(key='video_ids', value=video_ids)

    def extract_comments(**kwargs):
        """Fetch comments for the extracted videos."""
        # Pull video_ids from XCom
        video_ids = kwargs['ti'].xcom_pull(key='video_ids', task_ids='extract_videos')
        if not video_ids:
            print("No videos found.")
            return

        comments = fetch_youtube_comments(video_ids, API_KEY, max_results=5)
        print(f"Extracted comments: {comments}")

    # Define the tasks
    extract_videos_task = PythonOperator(
        task_id='extract_videos',
        python_callable=extract_videos,
        provide_context=True,
    )

    extract_comments_task = PythonOperator(
        task_id='extract_comments',
        python_callable=extract_comments,
        provide_context=True,
    )

    # Set task dependencies
    extract_videos_task >> extract_comments_task
