import pandas as pd
import uuid
from datetime import datetime

def create_time_info(timestamp_str):
    """
    Converts a timestamp string into time-related attributes.

    Args:
        timestamp_str (str): A timestamp string in the format '%Y-%m-%d %H:%M:%S'.

    Returns:
        tuple: A tuple containing:
            - time_id (int): Unix timestamp.
            - day_of_week (str): Full name of the day of the week.
            - timestamp (datetime): The parsed datetime object.
    """
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    time_id = int(timestamp.timestamp())
    day_of_week = timestamp.strftime('%A')
    return time_id, day_of_week, timestamp

def create_time_dim_df(data_list):
    """
    Creates a DataFrame for the time dimension from a list of data.

    Args:
        data_list (list): A list of dictionaries containing 'timestamp' keys.

    Returns:
        pd.DataFrame: A DataFrame with time dimension attributes.
    """
    time_dim_data = [
        {
            'time_id': time_id,
            'day_of_week': day_of_week,
            'time': timestamp
        }
        for data in data_list
        for time_id, day_of_week, timestamp in [create_time_info(data['timestamp'])]
    ]
    return pd.DataFrame(time_dim_data)

def create_post_dim_df(data_list, platform_id):
    """
    Creates a DataFrame for the post dimension from a list of data.

    Args:
        data_list (list): A list of dictionaries containing post details.
        platform_id (str): The platform identifier.

    Returns:
        pd.DataFrame: A DataFrame with post dimension attributes.
    """
    post_dim_data = [
        {
            'post_id': data['post_id'],
            'platform_id': platform_id,
            'title': data['title'],
            'description': data['body'],
            'url': data['url'],
            'engagement_score': data['score'],
            'time_id': create_time_info(data['timestamp'])[0],
            'version': datetime.now()
        }
        for data in data_list
    ]
    return pd.DataFrame(post_dim_data)

def create_user_dim_df(data_list, platform_id):
    """
    Creates a DataFrame for the user dimension from a list of data.

    Args:
        data_list (list): A list of dictionaries containing user details.
        platform_id (str): The platform identifier.

    Returns:
        pd.DataFrame: A DataFrame with user dimension attributes.
    """
    user_dim_data = [
        {
            'platform_id': platform_id,
            'username': data['author'],
            'version': datetime.now()
        }
        for data in data_list
    ]
    return pd.DataFrame(user_dim_data)

def create_comment_dim_df(data_list, platform_id):
    """
    Creates a DataFrame for the comment dimension from a list of data.

    Args:
        data_list (list): A list of dictionaries containing comment details.
        platform_id (str): The platform identifier.

    Returns:
        pd.DataFrame: A DataFrame with comment dimension attributes.
    """
    comment_dim_data = [
        {
            'comment_id': data['id'],
            'post_id': data['post_id'],
            'username': data['author'],
            'platform_id': platform_id,
            'parent_comment_id': data.get('parent_id', ''),
            'comment_text': data['body'],
            'comment_depth': data['depth'],
            'engagement_score': int(data['score']),
            'language': data['language'],
            'time_id': create_time_info(data['timestamp'])[0],
            'version': datetime.now()
        }
        for data in data_list
    ]
    return pd.DataFrame(comment_dim_data)

def create_sentiment_fact_df(data_list, platform_id):
    """
    Creates a DataFrame for sentiment facts from a list of data.

    Args:
        data_list (list): A list of dictionaries containing sentiment analysis details.
        platform_id (str): The platform identifier.

    Returns:
        pd.DataFrame: A DataFrame with sentiment fact attributes.
    """
    sentiment_fact_data = [
        {
            'sentiment_id': uuid.uuid4(),
            'comment_id': data['id'],
            'platform_id': platform_id,
            'time_id': create_time_info(data['timestamp'])[0],
            'sentiment_score': data['sentiment_score'],
            'sentiment_label': data['sentiment'],
            'version': datetime.now()
        }
        for data in data_list
    ]
    return pd.DataFrame(sentiment_fact_data)
