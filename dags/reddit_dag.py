import sys
import os
import asyncpraw
import logging
from airflow import DAG
import asyncio
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import the provided functions
sys.path.append('/opt/airflow')
from etl.extract import fetch_reddit_posts, fetch_reddit_comments
from etl.transform import clean_comments, label_language, label_sentiment

# Define your YouTube Data API key
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')
REDDIT_USERNAME = os.getenv('REDDIT_USERNAME')
REDDIT_PASSWORD = os.getenv('REDDIT_PASSWORD')

# Define default arguments for the DAG
default_args = {
    'owner': 'test',
    'depends_on_past': False,
}

# Initialize the DAG
with DAG(
    dag_id='reddit_ETL',
    default_args=default_args,
    description='DAG to extract YouTube videos and comments',
    schedule_interval=None,
    start_date=datetime(2024, 11, 16),
    catchup=False,
) as dag:

    def get_reddit(**kwargs):
        reddit = asyncpraw.Reddit(client_id=REDDIT_CLIENT_ID,
                                  client_secret=REDDIT_CLIENT_SECRET,
                                  user_agent=REDDIT_USER_AGENT,
                                  username=REDDIT_USERNAME,
                                  password=REDDIT_PASSWORD)
        return reddit

    async def async_extract_posts(ti, **kwargs):
        """Asynchronously extract Reddit posts."""
        reddit = get_reddit()
        list_subreddit = ['technology', 'phone', 'iphone', 'apple']
        list_keyword = ['iphone', 'ios', 'apple']
        posts = await fetch_reddit_posts(reddit, list_subreddit, list_keyword, 'relevance', 'day')
        posts_id = [post['post_id'] for post in posts]
        ti.xcom_push(key='posts', value=posts_id)
    
    async def async_extract_comments(ti, **kwargs):
        reddit = get_reddit()
        posts_id = ti.xcom_pull(key='posts', task_ids='extract_posts_task')
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
    
    def print_posts(**kwargs):
        post_id = kwargs['ti'].xcom_pull(key='posts', task_ids='extract_posts_task')
        if not post_id:
            logging.info("No videos found.")
            return
        else:
            logging.info(post_id[0])

    def print_comments(**kwargs):
        comments = kwargs['ti'].xcom_pull(key='comments', task_ids='extract_comments_task')
        if not comments:
            logging.info("No comments found.")
            return
        else:
            logging.info(comments[0])
    
    def clean_extracted_comments(**kwargs):
        """Clean the extracted comments using the clean_comments function."""
        ti = kwargs['ti']
        comments = ti.xcom_pull(key='comments', task_ids='extract_comments_task')
        if not comments:
            logging.info("No comments found to clean.")
            return
        cleaned_comments = clean_comments(comments)
        ti.xcom_push(key='cleaned_comments', value=cleaned_comments)

    def print_cleaned_comments(**kwargs):
        """Print the cleaned comments."""
        cleaned_comments = kwargs['ti'].xcom_pull(key='cleaned_comments', task_ids='clean_comments_task')
        if not cleaned_comments:
            logging.info("No cleaned comments found.")
            return
        else:
            logging.info(cleaned_comments[0])  # Print the first cleaned comment for brevity

    def label_language_comments(**kwargs):
        """Label the cleaned comments with detected languages."""
        ti = kwargs['ti']
        cleaned_comments = ti.xcom_pull(key='cleaned_comments', task_ids='clean_comments_task')
        if not cleaned_comments:
            logging.info("No cleaned comments found to label.")
            return
        labeled_comments = label_language(cleaned_comments)
        ti.xcom_push(key='labeled_comments', value=labeled_comments)

    def print_labeled_language(**kwargs):
        """Print the labeled comments."""
        labeled_comments = kwargs['ti'].xcom_pull(key='labeled_comments', task_ids='label_language_task')
        if not labeled_comments:
            logging.info("No labeled comments found.")
            return
        else:
            logging.info(labeled_comments[0])  # Print the first labeled comment for brevity
    
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

    def print_labeled_sentiment(**kwargs):
        """Print the sentiment-labeled comments."""
        labeled_comments = kwargs['ti'].xcom_pull(key='labeled_comments', task_ids='label_sentiment_task')
        
        if not labeled_comments:
            logging.info("No labeled comments found.")
            return
        else:
            logging.info(f"First labeled comment: {labeled_comments[0]}")

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

    print_comments_task = PythonOperator(
        task_id='print_comments_task',
        python_callable=print_comments,
        provide_context=True
    )

    print_posts_task = PythonOperator(
        task_id='print_posts_task',
        python_callable=print_posts,
        provide_context=True
    )


    # Define the new task for cleaning comments
    clean_comments_task = PythonOperator(
        task_id='clean_comments_task',
        python_callable=clean_extracted_comments,
        provide_context=True
    )

    # Define the new task for printing cleaned comments
    print_cleaned_comments_task = PythonOperator(
        task_id='print_cleaned_comments_task',
        python_callable=print_cleaned_comments,
        provide_context=True
    )

    # Define the new task for labeling comments
    label_language_task = PythonOperator(
        task_id='label_language_task',
        python_callable=label_language_comments,
        provide_context=True
    )

    # Define the new task for printing labeled comments
    print_labeled_language_task = PythonOperator(
        task_id='print_labeled_language_task',
        python_callable=print_labeled_language,
        provide_context=True
    )

    # Define the PythonOperator for labeling sentiment
    label_sentiment_task = PythonOperator(
        task_id='label_sentiment_task',
        python_callable=label_sentiment_comments,
        provide_context=True
    )

    # Define a task to print labeled comments
    print_labeled_sentiment_task = PythonOperator(
        task_id='print_labeled_sentiment_task',
        python_callable=print_labeled_sentiment,
        provide_context=True
    )

    # Update task dependencies
    extract_posts_task >> print_posts_task
    extract_posts_task >> extract_comments_task
    extract_comments_task >> clean_comments_task
    clean_comments_task >> label_language_task  # Run sentiment analysis after cleaning
    label_language_task >> label_sentiment_task
    label_sentiment_task >> print_labeled_sentiment_task  # Print the results after labeling


