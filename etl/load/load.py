from etl.transform.dataframe_transform import (
    create_comment_dim_df,
    create_sentiment_fact_df,
    create_post_dim_df,
    create_time_dim_df,
    create_user_dim_df
)

def save_data_comments(client, labeled_comments, platform_id):
    """
    Save the extracted comments data to ClickHouse.

    Args:
        client: The database client for interacting with ClickHouse.
        labeled_comments (list): A list of labeled comments with associated metadata.
        platform_id (str): The platform identifier (e.g., subreddit or platform name).

    Returns:
        None
    """
    if not labeled_comments:
        print("No comments found to save.")
        return

    # Create DataFrames for comments and associated dimensions
    comment_dim_df = create_comment_dim_df(labeled_comments, platform_id)
    user_dim_df = create_user_dim_df(labeled_comments, platform_id)
    time_dim_df = create_time_dim_df(labeled_comments)
    sentiment_fact_df = create_sentiment_fact_df(labeled_comments, platform_id)

    # Insert DataFrames into ClickHouse
    client.insert_df("comment_dim", comment_dim_df)
    client.insert_df("time_dim", time_dim_df)
    client.insert_df("user_dim", user_dim_df)
    client.insert_df("sentiment_fact", sentiment_fact_df)

def save_data_posts(client, posts, platform_id):
    """
    Save the extracted posts data to ClickHouse.

    Args:
        client: The database client for interacting with ClickHouse.
        posts (list): A list of posts with associated metadata.
        platform_id (str): The platform identifier (e.g., subreddit or platform name).

    Returns:
        None
    """
    if not posts:
        print("No posts found to save.")
        return

    # Create DataFrames for posts and associated dimensions
    post_dim_df = create_post_dim_df(posts, platform_id)
    time_dim_df = create_time_dim_df(posts)

    # Insert DataFrames into ClickHouse
    client.insert_df("post_dim", post_dim_df)
    client.insert_df("time_dim", time_dim_df)
