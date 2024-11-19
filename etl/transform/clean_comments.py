import re

def clean_comments(comments):
    """
    Cleans the 'textDisplay' field in a list of comment dictionaries by removing usernames and extra whitespace.

    Args:
        comments (list): A list of dictionaries containing comment details, including 'textDisplay'.

    Returns:
        list: A list of dictionaries with cleaned 'textDisplay' text.
    """
    def clean_text(text):
      # Remove markdown links (e.g., [text](url))
      text = re.sub(r'\[([^\]]+)\]\((https?:\/\/[^\s]+)\)', r'\1', text)
      # Remove any remaining URLs
      text = re.sub(r'https?:\/\/\S+', '', text)
      # Remove markdown quotes like "> " or "\\>"
      text = re.sub(r'\\?>\s*', '', text)
      # Remove Reddit usernames (starting with u/)
      text = re.sub(r'u/\w+', '', text)
      # Remove zero-width characters (like \u200d, \u200c, etc.)
      text = re.sub(r'[\u200b\u200c\u200d\u2060]', '', text)
      # Remove Twitter-style usernames (starting with @)
      text = re.sub(r'@\w+', '', text)
      # Remove non-breaking spaces and other Unicode whitespace characters
      text = re.sub(r'\xa0', ' ', text)
      # Remove extra newlines and collapse multiple spaces into one
      text = re.sub(r'\s+', ' ', text).strip()
      return text

    # Iterate over each comment and clean the 'textDisplay' field
    for comment in comments:
        if 'textDisplay' in comment:
            comment['textDisplay'] = clean_text(comment['textDisplay'])

    return comments
