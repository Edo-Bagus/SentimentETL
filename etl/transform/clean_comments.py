import re

def clean_comments(comments):
    """
    Cleans the 'textDisplay' field in a list of comment dictionaries by removing unwanted elements like URLs,
    usernames, and extra whitespace.

    Args:
        comments (list): A list of dictionaries containing comment details, including 'textDisplay'.

    Returns:
        list: A list of dictionaries with cleaned 'textDisplay' text.
    """

    def clean_text(text):
        """
        Cleans a text string by removing unwanted elements such as links, usernames, and extra spaces.

        Args:
            text (str): The input text to clean.

        Returns:
            str: The cleaned text.
        """
        # Patterns to remove unwanted elements
        patterns = [
            (r'\[([^\]]+)\]\((https?:\/\/[^\s]+)\)', r'\1'),  # Markdown links
            (r'https?:\/\/\S+', ''),                         # URLs
            (r'\\?>\s*', ''),                                # Markdown quotes
            (r'u/\w+', ''),                                  # Reddit usernames
            (r'[\u200b\u200c\u200d\u2060]', ''),             # Zero-width characters
            (r'@\w+', ''),                                   # Twitter-style usernames
            (r'@@\w+', ''),                                  # YouTube-style usernames
            (r'\xa0', ' '),                                  # Non-breaking spaces
            (r'\s+', ' ')                                    # Collapse multiple spaces
        ]
        for pattern, replacement in patterns:
            text = re.sub(pattern, replacement, text)
        return text.strip()  # Remove leading/trailing whitespace

    # Clean the 'textDisplay' field for each comment
    for comment in comments:
        if 'textDisplay' in comment:
            comment['textDisplay'] = clean_text(comment['textDisplay'])

    return comments
