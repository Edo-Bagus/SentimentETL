import asyncpraw
from datetime import datetime

async def fetch_reddit_posts(reddit, subreddits, keywords, sort, time_filter):
    posts = []
    seen_posts = set()  # Set to track seen post IDs

    # Ensure the Reddit client uses an async session correctly
    async with reddit:  # Assuming reddit is an asyncpraw Reddit instance
        for subreddit_name in subreddits:
            subreddit = await reddit.subreddit(subreddit_name)

            for keyword in keywords:
                async for submission in subreddit.search(keyword, limit=None, sort=sort, time_filter=time_filter):
                    if submission.id in seen_posts:
                        continue

                    post_data = {
                        'post_id': submission.id,
                        'title': submission.title,
                        'url': submission.url,
                        'subreddit': submission.subreddit.display_name,
                        'timestamp': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                        'score': submission.score,
                        'body': submission.selftext
                    }
                    posts.append(post_data)
                    seen_posts.add(submission.id)

    return posts

async def fetch_reddit_comments(reddit, post_ids):
    """
    Asynchronously fetches all comments (including nested replies) from multiple Reddit posts.

    Args:
    - reddit (asyncpraw.Reddit): The async PRAW Reddit instance.
    - post_ids (list): List of Reddit post IDs.
    - limit (int): The number of top-level comments to fetch per post.

    Returns:
    - List of all comments (including replies) with author, body, depth, timestamp, parent ID, post ID, and language label.
    """
    comments = []

    async def get_all_comments(comment, post_id, depth=0):
        """
        Asynchronously fetches all child comments with depth, timestamp, parent ID, post ID.
        """

        # Collect comment data
        comment_data = {
            'post_id': post_id,
            'id': comment.id,
            'score': comment.score,
            'author': comment.author.name if comment.author else '[deleted]',
            'body': comment.body,
            'depth': depth,
            'timestamp': datetime.utcfromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
            'parent_id': None if depth == 0 else comment.parent_id.split('_')[1],  # Extract parent comment ID
        }
        if(not (comment_data['author'] == '[deleted]')):
          comments.append(comment_data)


        # Recursively fetch replies (if any), increasing the depth
        for reply in comment.replies:
            await get_all_comments(reply, post_id, depth + 1)

    # Fetch comments for each post ID
    for post_id in post_ids:
        submission = await reddit.submission(id=post_id)
        await submission.comments.replace_more(limit=0)  # Flatten "MoreComments" to get the full tree

        # Asynchronously iterate through the top-level comments
        for top_level_comment in submission.comments:
            await get_all_comments(top_level_comment, post_id)

    return comments
