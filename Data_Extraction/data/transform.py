def transform(all_articles):
    transformed_data = []
    for data in all_articles:
        transformed_data.append({
            "title": data['title'],
            "author": data['author'],
            "published_at": data['publishedAt'],
            "description": data['description']
        })
    return transformed_data
