from transformers import pipeline, AutoTokenizer

def label_sentiment(comments):
    """Analyzes sentiment for a list of comments, using context from parent comments if available."""
    
    model_name = "cardiffnlp/twitter-roberta-base-sentiment"
    sentiment_pipeline = pipeline("sentiment-analysis", model=model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    comments_dict = {comment['id']: comment for comment in comments}

    label_mapping = {
        "LABEL_0": "negative",
        "LABEL_1": "neutral",
        "LABEL_2": "positive"
    }

    def truncate_text(text, max_length=512):
        """Truncates text to fit within the maximum token length for the model."""
        tokens = tokenizer(text, truncation=True, max_length=max_length, return_tensors='pt')
        return tokenizer.decode(tokens['input_ids'][0], skip_special_tokens=True)

    for comment in comments:
        parent_comment = comments_dict.get(comment['parent_id']) if comment['depth'] >= 1 else None

        if parent_comment:
            combined_text = f"{comment['body']} [CONTEXT: {parent_comment['body']}]"
            truncated_text = truncate_text(combined_text, max_length=512)
        else:
            truncated_text = truncate_text(comment['body'], max_length=512)

        sentiment_result = sentiment_pipeline(truncated_text)[0]
        sentiment_label = label_mapping[sentiment_result['label']]

        # Update the original dictionary in place
        comment['sentiment'] = sentiment_label
        comment['sentiment_score'] = sentiment_result['score']

    return comments
