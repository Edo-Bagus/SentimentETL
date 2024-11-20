import asyncio
from datetime import datetime
from asyncpraw.exceptions import RedditAPIException
from asyncpraw.models.reddit.more import MoreComments


async def fetch_reddit_posts(reddit, subreddits, keywords, sort="relevance", time_filter="all"):
    """
    Fetch posts from specified subreddits matching keywords with deduplication and retry logic.

    Args:
        reddit (asyncpraw.Reddit): The async PRAW Reddit instance.
        subreddits (list): List of subreddit names to search.
        keywords (list): List of keywords to search for.
        sort (str): Sorting method (default: "relevance").
        time_filter (str): Time filter for posts (default: "all").

    Returns:
        list: List of posts with metadata.
    """
    posts = []
    seen_posts = set()  # Track processed post IDs to avoid duplicates

    async with reddit:
        for subreddit_name in subreddits:
            subreddit = await reddit.subreddit(subreddit_name)

            for keyword in keywords:
                retries = 0
                while True:
                    try:
                        async for submission in subreddit.search(keyword, sort=sort, limit=30, time_filter=time_filter):
                            if submission.id in seen_posts:
                                continue

                            # Collect post data
                            post_data = {
                                'post_id': submission.id,
                                'title': submission.title,
                                'url': submission.url,
                                'timestamp': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                                'score': submission.score,
                                'body': submission.selftext,
                            }
                            posts.append(post_data)
                            seen_posts.add(submission.id)
                        break  # Exit retry loop on success

                    except RedditAPIException as e:
                        # Handle rate-limit errors with exponential backoff
                        if "RATELIMIT" in str(e).upper():
                            wait_time = (retries + 1) * 10
                            print(f"Rate limit hit. Retrying in {wait_time} seconds...")
                            await asyncio.sleep(wait_time)
                            retries += 1
                        else:
                            print(f"Reddit API error: {e}")
                            break  # Exit on non-rate-limit errors

                    except Exception as e:
                        # Log unexpected errors and exit
                        print(f"Unexpected error: {e}")
                        break

    return posts


async def fetch_reddit_comments(reddit, post_ids):
    """
    Fetch all comments (including nested replies) from specified posts.

    Args:
        reddit (asyncpraw.Reddit): The async PRAW Reddit instance.
        post_ids (list): List of Reddit post IDs.

    Returns:
        list: List of comments with metadata.
    """
    comments = []

    async def get_all_comments(comment, post_id, depth=0):
        """
        Recursively fetch child comments, collecting metadata.
        """
        if isinstance(comment, MoreComments):
            return  # Skip placeholders for additional comments

        # Collect comment data
        comment_data = {
            'post_id': post_id,
            'id': comment.id,
            'score': comment.score,
            'author': comment.author.name if comment.author else '[deleted]',
            'body': comment.body,
            'depth': depth,
            'timestamp': datetime.utcfromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
            'parent_id': None if depth == 0 else comment.parent_id.split('_')[1],
        }
        if comment_data['author'] != '[deleted]':  # Exclude deleted comments
            comments.append(comment_data)

        # Recursively process replies
        for reply in comment.replies:
            await get_all_comments(reply, post_id, depth + 1)

    # Fetch comments for each post ID
    for post_id in post_ids:
        submission = await reddit.submission(id=post_id)
        # Iterate through top-level comments
        for top_level_comment in submission.comments:
            await get_all_comments(top_level_comment, post_id)

    return comments
