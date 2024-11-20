from datetime import datetime

def format_timestamp(timestamp):
    """
    Converts an ISO 8601 timestamp to a formatted string.

    Args:
        timestamp (str): ISO 8601 timestamp (e.g., "2024-11-20T12:34:56Z").

    Returns:
        str: Formatted timestamp as "YYYY-MM-DD HH:MM:SS".
    """
    dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    return dt.strftime("%Y-%m-%d %H:%M:%S")
