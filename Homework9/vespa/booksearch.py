import pandas as pd
from vespa.application import Vespa
from vespa.io import VespaQueryResponse

def display_hits_as_df(response: VespaQueryResponse, fields) -> pd.DataFrame:
    records = []
    for hit in response.hits:
        record = {}
        for field in fields:
            record[field] = hit["fields"].get(field, "")
        records.append(record)
    return pd.DataFrame(records)

def keyword_search(app, search_query):
    query = {
        "yql": "select * from sources * where userQuery() limit 5",
        "query": search_query,
        "ranking": "bm25",
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title"])

# Placeholder for Semantic Search (requires embeddings)
def semantic_search(app, search_query):
    query = {
        "yql": "select * from sources * where ({targetHits:100}nearestNeighbor(embedding, e)) limit 5",
        "query": search_query,
        "ranking": "semantic",
        "input.query(e)": "embed(@query)"  # Placeholder for embedding
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title"])

def get_document_details(doc_id):
    query = {
        "yql": f"select doc_id, title, text from content.doc where doc_id contains '{doc_id}'",
        "hits": 1
    }
    result = app.query(query)
    
    if result.hits:
        hit = result.hits[0]
        details = {
            "doc_id": hit["fields"]["doc_id"],
            "title": hit["fields"]["title"],
            "text": hit["fields"]["text"][:100] + "..." if len(hit["fields"]["text"]) > 100 else hit["fields"]["text"]
        }
        return details
    return None

# Placeholder for Recommendations by Embedding (requires embeddings)
def query_products_by_embedding(app, embedding_vector):
    query = {
        'hits': 5,
        'yql': 'select * from content.doc where ({targetHits:5}nearestNeighbor(embedding, user_embedding))',
        'ranking.features.query(user_embedding)': str(embedding_vector),
        'ranking.profile': 'recommendation'
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title", "text"])

# Connect to Vespa instance
app = Vespa(url="http://localhost", port=8080)

# Example usage of keyword search
query = "To Kill a Mockingbird"
df = keyword_search(app, query)
print("Keyword Search Results:")
print(df)

# Example usage of semantic search (won't work without embeddings)
df = semantic_search(app, query)
print("\nSemantic Search Results:")
print(df)

# Example document details (replace doc_id with a known value)
doc_id = "0"  # Replace with an actual doc_id if known
#document_details = get_document_details(doc_id)
'''if document_details:
    print("\nDocument Details:")
    for key, value in document_details.items():
        print(f"{key}: {value}")
else:
    print(f"No document found for ID: {doc_id}")'''

# Example recommendation by embedding (won't work without embeddings)
embedding_vector = [0.1] * 128  # Placeholder for an actual embedding vector
df = query_products_by_embedding(app, embedding_vector)
print("\nRecommendations based on Embedding:")
print(df)
