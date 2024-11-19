from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime

def fetch_youtube_videos(query, api_key, max_results=10, all_pages=False):
    """
    Searches YouTube for videos based on a query and returns video details.

    Parameters:
    - query (str): The search query.
    - api_key (str): Your YouTube Data API key.
    - max_results (int): The maximum number of results per page (default is 10).
    - all_pages (bool): Whether to fetch all pages or just the first page (default is False).

    Returns:
    - List[dict]: A list of dictionaries containing video details, such as video ID, title, description, URL, and publish time.
    """
    # Initialize the YouTube Data API client
    youtube = build("youtube", "v3", developerKey=api_key)

    # Get the start of the current day in UTC and convert it to RFC 3339 format
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    published_after = today_start.isoformat() + "Z"  # Convert to RFC 3339 format

    # List to hold all video details
    videos = []

    # Initialize the search request for the first page of results
    request = youtube.search().list(
        q=query,
        part="id,snippet",
        type="video",
        maxResults=max_results,
        publishedAfter=published_after
    )

    # Loop to process multiple pages of results if needed
    while request:
        # Execute the request and get the response
        response = request.execute()

        # Extract video details from the response
        for item in response.get("items", []):
            video_id = item["id"]["videoId"]
            title = item["snippet"]["title"]
            description = item["snippet"]["description"]
            publish_time_str = item["snippet"]["publishedAt"]
            
            # Append video details to the list
            videos.append({
                "video_id": video_id,
                "title": title,
                "description": description,
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "publishTime": publish_time_str
            })

        # If only the first page is needed, break out of the loop
        if not all_pages:
            break

        # If there is a next page, update the request with the nextPageToken
        if "nextPageToken" in response:
            request = youtube.search().list(
                q=query,
                part="id,snippet",
                type="video",
                maxResults=max_results,
                publishedAfter=published_after,
                pageToken=response["nextPageToken"]
            )
        else:
            # No more pages available, exit the loop
            request = None

    return videos

def fetch_youtube_comments(video_ids, api_key, max_results=5):
    """
    Fetches top-level comments and their replies for a list of YouTube videos.

    Parameters:
    - video_ids (list): List of YouTube video IDs.
    - api_key (str): Your YouTube Data API key.
    - max_results (int): Maximum number of comments and replies to fetch per request.

    Returns:
    - List[dict]: A list of dictionaries containing comment details, including language.
    """
    youtube = build("youtube", "v3", developerKey=api_key)
    all_comments = []

    for video_id in video_ids:
        try:
            # Step 1: Fetch top-level comments for the current video
            top_level_request = youtube.commentThreads().list(
                part="id,snippet",
                videoId=video_id,
                textFormat="plainText",
                order="relevance",
                maxResults=max_results
            )
            top_level_response = top_level_request.execute()

            for item in top_level_response.get("items", []):
                # Extract top-level comment details
                top_comment = item["snippet"]["topLevelComment"]["snippet"]
                comment_id = item["id"]
                text = top_comment["textOriginal"]
                likes = top_comment.get("likeCount", 0)  # Get like count (score)

                all_comments.append({
                    "video_id": video_id,
                    "comment_id": comment_id,
                    "text": text,
                    "author": top_comment.get("authorDisplayName"),
                    "publish_time": top_comment["publishedAt"],
                    "depth": 0,
                    "parent_id": None,
                    "score": likes  # Add score (likes)
                })

                # Step 2: Fetch replies to the top-level comment
                if item["snippet"]["totalReplyCount"] > 0:
                    reply_request = youtube.comments().list(
                        part="snippet",
                        parentId=comment_id,
                        textFormat="plainText",
                        maxResults=max_results
                    )
                    reply_response = reply_request.execute()

                    for reply_item in reply_response.get("items", []):
                        reply = reply_item["snippet"]
                        reply_text = reply["textOriginal"]
                        reply_likes = reply.get("likeCount", 0)  # Get like count (score)

                        all_comments.append({
                            "video_id": video_id,
                            "comment_id": reply_item["id"],
                            "text": reply_text,
                            "author": reply.get("authorDisplayName"),
                            "publish_time": reply["publishedAt"],
                            "depth": 1,
                            "parent_id": comment_id,
                            "score": reply_likes  # Add score (likes) for the reply
                        })

        except HttpError as e:
            error_reason = e.error_details[0]['reason'] if 'reason' in e.error_details[0] else 'unknown'
            if error_reason == 'commentsDisabled':
                print(f"Comments are disabled for video ID: {video_id}. Skipping...")
            else:
                print(f"An error occurred for video ID {video_id}: {e}")
            continue

    return all_comments