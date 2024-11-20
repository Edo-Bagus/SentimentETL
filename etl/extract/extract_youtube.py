from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
from etl.transform import format_timestamp


def fetch_youtube_videos(queries, api_key, max_results=50, all_pages=False):
    """
    Fetch YouTube videos for given queries using YouTube Data API.

    Args:
        queries (list): List of search queries.
        api_key (str): YouTube Data API key.
        max_results (int): Maximum results per page (default: 50).
        all_pages (bool): Fetch all pages if True, otherwise only first page.

    Returns:
        list[dict]: List of video details (ID, title, description, URL, etc.).
    """
    youtube = build("youtube", "v3", developerKey=api_key)
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    published_after = today_start.isoformat() + "Z"
    all_videos = []

    for query in queries:
        videos = []
        request = youtube.search().list(
            q=query, part="id,snippet", type="video", maxResults=max_results, publishedAfter=published_after
        )

        # Paginate through results if required
        while request:
            response = request.execute()
            for item in response.get("items", []):
                video_id = item["id"]["videoId"]
                videos.append({
                    "post_id": video_id,
                    "title": item["snippet"]["title"],
                    "body": item["snippet"]["description"],
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "timestamp": format_timestamp(item["snippet"]["publishedAt"]),
                    "score": 0
                })

            if not all_pages or "nextPageToken" not in response:
                break

            request = youtube.search().list(
                q=query, part="id,snippet", type="video", maxResults=max_results,
                publishedAfter=published_after, pageToken=response["nextPageToken"]
            )

        all_videos.extend(videos)

    return all_videos


def fetch_youtube_comments(video_ids, api_key, max_results=100):
    """
    Fetch comments (top-level and replies) for given video IDs.

    Args:
        video_ids (list): List of video IDs.
        api_key (str): YouTube Data API key.
        max_results (int): Maximum comments per request (default: 100).

    Returns:
        list[dict]: List of comments with details (ID, author, body, timestamp, etc.).
    """
    youtube = build("youtube", "v3", developerKey=api_key)
    all_comments = []

    for video_id in video_ids:
        try:
            # Fetch top-level comments
            top_level_request = youtube.commentThreads().list(
                part="id,snippet", videoId=video_id, textFormat="plainText", order="relevance", maxResults=max_results
            )
            top_level_response = top_level_request.execute()

            for item in top_level_response.get("items", []):
                top_comment = item["snippet"]["topLevelComment"]["snippet"]
                comment_id = item["id"]
                all_comments.append({
                    "post_id": video_id,
                    "id": comment_id,
                    "body": top_comment["textOriginal"],
                    "author": top_comment.get("authorDisplayName"),
                    "timestamp": format_timestamp(top_comment["publishedAt"]),
                    "depth": 0,
                    "parent_id": '',
                    "score": top_comment.get("likeCount", 0)
                })

                # Fetch replies to the top-level comment if available
                if item["snippet"]["totalReplyCount"] > 0:
                    reply_request = youtube.comments().list(
                        part="snippet", parentId=comment_id, textFormat="plainText", maxResults=max_results
                    )
                    reply_response = reply_request.execute()
                    for reply_item in reply_response.get("items", []):
                        reply = reply_item["snippet"]
                        all_comments.append({
                            "post_id": video_id,
                            "id": reply_item["id"],
                            "body": reply["textOriginal"],
                            "author": reply.get("authorDisplayName"),
                            "timestamp": format_timestamp(reply["publishedAt"]),
                            "depth": 1,
                            "parent_id": comment_id,
                            "score": reply.get("likeCount", 0)
                        })

        except HttpError as e:
            error_reason = e.error_details[0].get('reason', 'unknown')
            if error_reason == 'commentsDisabled':
                print(f"Comments are disabled for video ID: {video_id}. Skipping...")
            else:
                print(f"An error occurred for video ID {video_id}: {e}")
            continue

    return all_comments
