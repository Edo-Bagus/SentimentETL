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
from etl.extract import fetch_reddit_posts, fetch_reddit_comments
from etl.transform import clean_comments, label_language, label_sentiment
from etl.load import save_data_posts, save_data_comments

# Define your YouTube Data API key
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')
REDDIT_USERNAME = os.getenv('REDDIT_USERNAME')
REDDIT_PASSWORD = os.getenv('REDDIT_PASSWORD')

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
    dag_id='reddit_ETL_populate',
    default_args=default_args,
    description='DAG to extract Reddit post and comments for the first time',
    schedule_interval=None,  # Still specified in local timezone
    start_date=datetime(2024, 11, 19),
    catchup=False,
) as dag:

    def get_reddit(**kwargs):
        reddit = asyncpraw.Reddit(client_id=REDDIT_CLIENT_ID,
                                  client_secret=REDDIT_CLIENT_SECRET,
                                  user_agent=REDDIT_USER_AGENT,
                                  username=REDDIT_USERNAME,
                                  password=REDDIT_PASSWORD)
        return reddit
    
    def get_clickhouse_client():
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            secure=CLICKHOUSE_SECURE
        )

        return client

    async def async_extract_posts(ti, **kwargs):
        """Asynchronously extract Reddit posts."""
        reddit = get_reddit()
        list_subreddit = [
            'technology', 
            'iphone', 
            'apple', 
            'smartphones', 
            'mobile'
        ]

        list_keyword = [
            'iphone 15 14 13'
            'iphone case',
            'iphone price'
            'iphone battery', 
            'iphone camera', 
            'iphone performance', 
            'iphone durability', 
            'iphone software hardware'
        ]
        posts = await fetch_reddit_posts(reddit, list_subreddit, list_keyword, 'relevance', 'all')
        ti.xcom_push(key='posts', value=posts)
    
    async def async_extract_comments(ti, **kwargs):
        reddit = get_reddit()
        posts = ti.xcom_pull(key='posts', task_ids='extract_posts_task')
        posts_id = [post['post_id'] for post in posts]
        comments = await fetch_reddit_comments(reddit, posts_id)
        ti.xcom_push(key='comments', value = comments)

    def extract_posts(**kwargs):
        """Wrapper to run the async function using the event loop."""
        ti = kwargs['ti']
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If there's an existing event loop, create a task
            task = loop.create_task(async_extract_posts(ti))
            loop.run_until_complete(task)
        else:
            # If no event loop, create a new one
            asyncio.run(async_extract_posts(ti))

    def extract_comments(**kwargs):
        """Wrapper to run the async function using the event loop."""
        ti = kwargs['ti']
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If there's an existing event loop, create a task
            task = loop.create_task(async_extract_comments(ti))
            loop.run_until_complete(task)
        else:
            # If no event loop, create a new one
            asyncio.run(async_extract_comments(ti))
    
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
        posts = kwargs['ti'].xcom_pull(key='posts', task_ids='extract_posts_task')
        save_data_posts(client, posts, 1)
    
    def load_data_comments(**kwargs):
        client = get_clickhouse_client()
        labeled_comments = kwargs['ti'].xcom_pull(key='labeled_comments', task_ids='label_sentiment_task')
        save_data_comments(client, labeled_comments, 1)

    extract_posts_task = PythonOperator(
        task_id='extract_posts_task',
        python_callable=extract_posts,
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
    extract_posts_task >> extract_comments_task
    extract_posts_task >> save_data_posts_task
    extract_comments_task >> clean_comments_task
    clean_comments_task >> label_language_task  # Run sentiment analysis after cleaning
    label_language_task >> label_sentiment_task # Print the results after labeling
    label_sentiment_task >> save_data_comments_task


