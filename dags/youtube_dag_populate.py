import sys
import os
import uuid
import asyncpraw
import logging
from airflow import DAG
import asyncio
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import clickhouse_connect

# Import the provided functions
sys.path.append('/opt/airflow')
from etl.extract import fetch_youtube_videos, fetch_youtube_comments
from etl.transform import clean_comments, label_language, label_sentiment
from etl.load import save_data_posts, save_data_comments

# Define your YouTube Data API key
API_KEY = os.getenv('API_KEY')

# Fetch the credentials from environment variables
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_SECURE = os.getenv('CLICKHOUSE_SECURE') == 'True'

# Define default arguments for the DAG
default_args = {
    'owner': 'test',
    'depends_on_past': False,
}

# Initialize the DAG
with DAG(
    dag_id='youtube_ETL_populate',
    default_args=default_args,
    description='DAG to extract YouTube videos and comments and populate it for the first time',
    schedule_interval=None,  # Still specified in local timezone
    start_date=datetime(2024, 11, 19),
    catchup=False,
) as dag:
    
    def get_clickhouse_client():
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            secure=CLICKHOUSE_SECURE
        )

        return client

    def extract_videos(**kwargs):
        """Extract YouTube videos based on a query."""
        queries = [
            # Hardware-Related Queries
            "iPhone performance review",
            "iPhone overheating issues",
            "iPhone battery life feedback",
            "iPhone case durability",
            "Magsafe accessories opinions",
            "iPhone screen repair feedback",
            "iPhone vs Android hardware reviews",
            "best smartphone cameras 2024",
            "iPhone durability tests",
            
            # Software-Related Queries
            "iOS 17 bugs and reviews",
            "new iOS features opinions",
            "iOS vs Android user experience",
            "iPhone app crashing issue",
            "iPhone software review",
            "Siri vs Google Assistant usability",
            "Face ID reliability reviews",
            "iOS smoothness feedback",
            "iPhone UI satisfaction",
            "Apple ecosystem user reviews",
            
            # General Queries
            "Apple iPhone brand perception",
            "why people love iPhones",
            "why people hate iPhones",
            "iPhone 15 keynote reactions",
            "iPhone price review",
        ]
        videos = fetch_youtube_videos(queries, API_KEY)
        print(f"Extracted video IDs: {videos}")
        # Push video_ids to XCom for the next task
        kwargs['ti'].xcom_push(key='videos', value=videos)

    def extract_comments(**kwargs):
        """Fetch comments for the extracted videos."""
        # Pull video_ids from XCom
        videos = kwargs['ti'].xcom_pull(key='videos', task_ids='extract_videos_task')
        video_ids = [video['post_id'] for video in videos]
        if not video_ids:
            print("No videos found.")
            return

        comments = fetch_youtube_comments(video_ids, API_KEY)
        kwargs['ti'].xcom_push(key='comments', value=comments)
        print(f"Extracted comments: {comments}")

    def clean_extracted_comments(**kwargs):
        """Clean the extracted comments using the clean_comments function."""
        ti = kwargs['ti']
        comments = ti.xcom_pull(key='comments', task_ids='extract_comments_task')
        if not comments:
            logging.info("No comments found to clean.")
            return
        cleaned_comments = clean_comments(comments)
        ti.xcom_push(key='cleaned_comments', value=cleaned_comments)

    def label_language_comments(**kwargs):
        """Label the cleaned comments with detected languages."""
        ti = kwargs['ti']
        cleaned_comments = ti.xcom_pull(key='cleaned_comments', task_ids='clean_comments_task')
        if not cleaned_comments:
            logging.info("No cleaned comments found to label.")
            return
        labeled_comments = label_language(cleaned_comments)
        ti.xcom_push(key='labeled_comments', value=labeled_comments)

    def label_sentiment_comments(**kwargs):
        """Task to label comments with sentiment using the analyze_comments_with_sentiment function."""
        ti = kwargs['ti']
        cleaned_comments = ti.xcom_pull(key='labeled_comments', task_ids='label_language_task')
        
        if not cleaned_comments:
            logging.info("No comments found to label.")
            return

        # Perform sentiment analysis on the cleaned comments
        labeled_comments = label_sentiment(cleaned_comments)
        
        # Push the labeled comments to XCom
        ti.xcom_push(key='labeled_comments', value=labeled_comments)

    def load_data_posts(**kwargs):
        client = get_clickhouse_client()
        videos = kwargs['ti'].xcom_pull(key='videos', task_ids='extract_videos_task')
        save_data_posts(client, videos, 2)
    
    def load_data_comments(**kwargs):
        client = get_clickhouse_client()
        labeled_comments = kwargs['ti'].xcom_pull(key='labeled_comments', task_ids='label_sentiment_task')
        save_data_comments(client, labeled_comments, 2)

    # Define the tasks
    extract_videos_task = PythonOperator(
        task_id='extract_videos_task',
        python_callable=extract_videos,
        provide_context=True,
    )

    extract_comments_task = PythonOperator(
        task_id='extract_comments_task',
        python_callable=extract_comments,
        provide_context=True,
    )

    # Define the new task for cleaning comments
    clean_comments_task = PythonOperator(
        task_id='clean_comments_task',
        python_callable=clean_extracted_comments,
        provide_context=True
    )

     # Define the new task for labeling comments
    label_language_task = PythonOperator(
        task_id='label_language_task',
        python_callable=label_language_comments,
        provide_context=True
    )

     # Define the PythonOperator for labeling sentiment
    label_sentiment_task = PythonOperator(
        task_id='label_sentiment_task',
        python_callable=label_sentiment_comments,
        provide_context=True
    )

    save_data_comments_task = PythonOperator(
        task_id='save_data_comments_task',
        python_callable=load_data_comments,
        provide_context=True
    )

    save_data_posts_task = PythonOperator(
        task_id='save_data_posts_task',
        python_callable=load_data_posts,
        provide_context=True
    )

    # Update task dependencies
    extract_videos_task >> extract_comments_task
    extract_videos_task >> save_data_posts_task
    extract_comments_task >> clean_comments_task
    clean_comments_task >> label_language_task  # Run sentiment analysis after cleaning
    label_language_task >> label_sentiment_task # Print the results after labeling
    label_sentiment_task >> save_data_comments_task
