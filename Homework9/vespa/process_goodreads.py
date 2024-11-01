import pandas as pd
import json

# Define a function to combine relevant text features into a single text field
def combine_book_features(row):
    try:
        return f"{row['Book']} {row['Description']} {row['Genres']}"
    except Exception as e:
        print("Error:", e)
        return ""

# Process the Goodreads CSV data and save it as JSONL
def process_goodreads_csv(input_file, output_file):
    # Load CSV file
    books = pd.read_csv(input_file)
    
    # Fill NaN values in relevant columns
    for column in ['Book', 'Description', 'Genres']:
        books[column] = books[column].fillna('')
    
    # Create a 'text' column for Vespa using the combined features
    books["text"] = books.apply(combine_book_features, axis=1)
    
    # Select and rename columns for Vespa compatibility
    books = books[['index_id', 'Book', 'text']]
    books.rename(columns={'index_id': 'doc_id', 'Book': 'title'}, inplace=True)
    
    # Create 'fields' column with JSON-like structure for each record
    books['fields'] = books.apply(lambda row: row.to_dict(), axis=1)
    
    # Create 'put' column based on 'doc_id' to uniquely identify each document
    books['put'] = books['doc_id'].apply(lambda x: f"id:goodreads-books:doc::{x}")
    
    # Save to JSONL file
    df_result = books[['put', 'fields']]
    df_result.to_json(output_file, orient='records', lines=True)

# Execute the function
process_goodreads_csv('goodreads.csv', 'goodreads_reviews.jsonl')