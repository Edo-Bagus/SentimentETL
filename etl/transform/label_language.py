from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

def detect_language(text):
    """
    Detects the language of a given text using langdetect.

    Args:
        text (str): The text to analyze.

    Returns:
        str: The detected language code (e.g., 'en', 'es', 'fr'). Returns 'unknown' if detection fails.
    """
    try:
        return detect(text)
    except LangDetectException:
        return 'unknown'

def label_language(comments):
    """
    Adds a language label to each comment dictionary based on its 'body' field.

    Args:
        comments (list): A list of dictionaries containing comment details, including 'body'.

    Returns:
        list: A list of dictionaries with an added 'language' key.
    """
    for comment in comments:
        comment['language'] = detect_language(comment.get('body', ''))
    return comments
