from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

def detect_language(text):
    """
    Detects the language of a given text using langdetect.

    Parameters:
    - text (str): The text to detect the language of.

    Returns:
    - str: The detected language code (e.g., 'en', 'es', 'fr'). Returns 'unknown' if detection fails.
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
        # Detect language only if 'body' exists
        if 'body' in comment and comment['body']:
            comment['language'] = detect_language(comment['body'])
        else:
            comment['language'] = 'unknown'
    return comments
