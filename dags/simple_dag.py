from airflow import DAG
import sys
import os
import asyncio
import asyncpraw
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import clickhouse_connect

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

# Function to create time_id and day_of_week
def create_time_info(timestamp_str):
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    time_id = int(timestamp.timestamp())  # Unix timestamp
    day_of_week = timestamp.strftime('%A')  # Full day name
    return time_id, day_of_week, timestamp

# Prepare post_dim DataFrame for a list of posts
def create_post_dim_df(data_list):
    post_dim_data = []
    for data in data_list:
        platform_id = 1  # Placeholder platform ID, could be based on subreddit
        engagement_score = data['score']  # Using score as engagement score
        time_id = create_time_info(data['timestamp'])[0]  # Add time_id from the timestamp
        
        post_dim_data.append({
            'post_id': data['post_id'],
            'platform_id': platform_id,
            'title': data['title'],
            'description': data['body'],
            'url': data['url'],
            'engagement_score': engagement_score,
            'time_id': time_id
        })
    
    return pd.DataFrame(post_dim_data)

# Prepare time_dim DataFrame for a list of posts
def create_time_dim_df(data_list):
    time_dim_data = []
    for data in data_list:
        time_info = create_time_info(data['timestamp'])
        
        time_dim_data.append({
            'time_id': time_info[0],
            'day_of_week': time_info[1],
            'time': time_info[2]
        })
    
    return pd.DataFrame(time_dim_data)

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
        posts = await fetch_reddit_posts(reddit, list_subreddit, list_keyword, 'relevance', 'all')
        ti.xcom_push(key='posts', value=posts)

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

def save_data(ti, **kwargs):
        """Save the extracted data to ClickHouse."""
        posts = ti.xcom_pull(key='posts', task_ids='extract_posts_task')
        if not posts:
            print("No posts found to save.")
            return
       

        # Create DataFrames
        post_dim_df = create_post_dim_df(posts)
        time_dim_df = create_time_dim_df(posts)

        client = clickhouse_connect.get_client(
            host='mzm4dqeeiq.ap-southeast-1.aws.clickhouse.cloud',
            user='default',
            password='eX.lOav00gzCE',
            secure=True
        )

        client.insert_df("post_dim", post_dim_df)
        client.insert_df("time_dim", time_dim_df)

with DAG(
    'simple_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_posts_task = PythonOperator(
        task_id='extract_posts_task',
        python_callable=extract_posts,
        provide_context=True,
    )

    save_data_task = PythonOperator(
        task_id='save_data_task',
        python_callable=save_data,
        provide_context=True
    )

    extract_posts_task >> save_data_task
