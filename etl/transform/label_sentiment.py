from transformers import pipeline, AutoTokenizer

def label_sentiment(comments, batch_size=32):
    """
    Analyzes sentiment for a list of comments, using context from parent comments if available.
    Processes comments in batches for better performance.

    Args:
        comments (list): A list of dictionaries containing comment details.
        batch_size (int): The number of comments to process in each batch.

    Returns:
        list: The updated list of comments with added 'sentiment' and 'sentiment_score' keys.
    """
    model_name = "cardiffnlp/twitter-roberta-base-sentiment"
    sentiment_pipeline = pipeline("sentiment-analysis", model=model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    # Mapping model labels to human-readable sentiment labels
    label_mapping = {
        "LABEL_0": "negative",
        "LABEL_1": "neutral",
        "LABEL_2": "positive"
    }

    def truncate_text(text, max_length=512):
        """
        Truncates text to fit within the model's maximum token length.

        Args:
            text (str): The text to truncate.
            max_length (int): The maximum allowed length in tokens.

        Returns:
            str: The truncated text.
        """
        tokens = tokenizer(text, truncation=True, max_length=max_length, return_tensors='pt')
        return tokenizer.decode(tokens['input_ids'][0], skip_special_tokens=True)

    # Prepare texts for sentiment analysis, including parent context if available
    comments_dict = {comment['id']: comment for comment in comments}
    processed_texts = []

    for comment in comments:
        parent_comment = comments_dict.get(comment.get('parent_id')) if comment.get('depth', 0) >= 1 else None
        if parent_comment:
            combined_text = f"{comment['body']} [CONTEXT: {parent_comment['body']}]"
        else:
            combined_text = comment.get('body', '')
        truncated_text = truncate_text(combined_text, max_length=512)
        processed_texts.append((comment, truncated_text))

    # Batch processing for sentiment analysis
    for i in range(0, len(processed_texts), batch_size):
        batch = processed_texts[i:i + batch_size]
        texts_to_analyze = [text for _, text in batch]
        sentiment_results = sentiment_pipeline(texts_to_analyze)

        # Update comments with sentiment analysis results
        for (comment, _), sentiment_result in zip(batch, sentiment_results):
            comment['sentiment'] = label_mapping.get(sentiment_result['label'], "unknown")
            comment['sentiment_score'] = sentiment_result['score']

    return comments
