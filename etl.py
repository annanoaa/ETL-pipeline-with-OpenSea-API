import os
import json
import time
from typing import List, Dict, Any
import requests
from orm import Database
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
API_KEY = os.environ.get('OPENSEA_API_KEY')
if not API_KEY:
    raise ValueError("OPENSEA_API_KEY not found in .env file")

DB_TYPE = 'sqlite'
DATABASE = 'opensea_collections.sqlite'

RAW_DATA_DIR = 'data_lake/raw'
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# ORM Initialization
db = Database(DB_TYPE, database=DATABASE)

# Table Schema
TABLE_NAME = 'collections'
SCHEMA = {
    'collection': 'TEXT',
    'name': 'TEXT',
    'description': 'TEXT',
    'image_url': 'TEXT',
    'owner': 'TEXT',
    'twitter_username': 'TEXT',
    'contracts': 'TEXT'
}


def create_table():
    db.create_table(TABLE_NAME, SCHEMA)


def extract_data() -> List[Dict[str, Any]]:
    url = 'https://api.opensea.io/api/v2/collections'
    headers = {'X-API-KEY': API_KEY}
    params = {'chain': 'ethereum'}
    all_collections = []
    next_cursor = None

    while True:
        if next_cursor:
            params['next'] = next_cursor
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(f"API request failed: {response.status_code} - {response.text}")
        data = response.json()
        collections = data.get('collections', [])
        all_collections.extend(collections)
        next_cursor = data.get('next')
        if not next_cursor:
            break
        time.sleep(1)

    # Save raw data
    raw_file = os.path.join(RAW_DATA_DIR, f'collections_{int(time.time())}.json')
    with open(raw_file, 'w') as f:
        json.dump(all_collections, f)

    return all_collections


def transform_data(collections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    transformed = []
    for coll in collections:
        transformed_row = {
            'collection': coll.get('name'),
            'name': coll.get('name'),
            'description': coll.get('description'),
            'image_url': coll.get('image_url'),
            'owner': coll.get('owner', {}).get('address', '') if isinstance(coll.get('owner'), dict) else '',
            'twitter_username': coll.get('twitter_username'),
            'contracts': json.dumps(coll.get('primary_asset_contracts', []))
        }
        transformed.append(transformed_row)
    return transformed


def load_data(transformed_data: List[Dict[str, Any]]):
    db.insert(TABLE_NAME, transformed_data)


def etl_pipeline():
    create_table()
    raw_collections = extract_data()
    transformed_data = transform_data(raw_collections)
    load_data(transformed_data)
    db.close()


if __name__ == '__main__':
    etl_pipeline()