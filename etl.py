import os
import json
import time
from typing import List, Dict, Any
import requests
from orm import Database
from dotenv import load_dotenv


class OpenSeaETL:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()
        self.api_key = os.getenv('OPENSEA_API_KEY')
        if not self.api_key:
            raise ValueError("OPENSEA_API_KEY not found in .env file")

        # Initialize the ORM with SQLite
        self.db = Database('sqlite', database='opensea_collections.sqlite')

        # Directory to store raw data
        self.raw_data_dir = 'data_lake/raw'
        os.makedirs(self.raw_data_dir, exist_ok=True)

        # Table configuration
        self.table_name = 'collections'
        self.schema = {
            'collection': 'TEXT',
            'name': 'TEXT',
            'description': 'TEXT',
            'image_url': 'TEXT',
            'owner': 'TEXT',
            'twitter_username': 'TEXT',
            'contracts': 'TEXT'
        }

    def extract_data(self) -> List[Dict[str, Any]]:
        # Base URL sets chain=ethereum and limit=30 per request.
        base_url = "https://api.opensea.io/api/v2/collections?chain=ethereum&limit=30"
        headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        all_collections = []
        next_cursor = None
        # Initial query parameters
        params = {"chain": "ethereum", "limit": 30}

        while True:
            if next_cursor:
                params["next"] = next_cursor

            response = requests.get(base_url, headers=headers, params=params)
            if response.status_code != 200:
                raise Exception(f"API request failed: {response.status_code} - {response.text}")

            data = response.json()
            collections = data.get('collections', [])
            all_collections.extend(collections)

            # Save raw data batch for this request to the data lake
            raw_file = os.path.join(self.raw_data_dir, f'collections_batch_{int(time.time())}.json')
            with open(raw_file, 'w') as f:
                json.dump(collections, f, indent=4)

            next_cursor = data.get('next')
            # Stop when no further pages or we have reached 90 collections as an example limit.
            if not next_cursor or len(all_collections) >= 90:
                break

            time.sleep(1)  # Respect rate limits

        return all_collections

    def transform_data(self, collections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Transform each collection record to match the table schema.
        return [{
            'collection': coll.get('name', ''),
            'name': coll.get('name', ''),
            'description': coll.get('description', ''),
            'image_url': coll.get('image_url', ''),
            'owner': coll.get('owner', {}).get('address', '') if isinstance(coll.get('owner'), dict) else '',
            'twitter_username': coll.get('twitter_username', ''),
            'contracts': json.dumps(coll.get('primary_asset_contracts', []))
        } for coll in collections]

    def run_pipeline(self):
        # Create the table using the defined schema.
        self.db.create_table(self.table_name, self.schema)

        # Extract data from the OpenSea API
        raw_data = self.extract_data()

        # Transform the raw data
        transformed_data = self.transform_data(raw_data)

        # Load transformed data into the database
        self.db.insert(self.table_name, transformed_data)

        # Verify the loaded data with a sample SELECT query
        sample = self.db.select(self.table_name, limit=5)
        print(f"Loaded {len(transformed_data)} collections. Sample:")
        for row in sample:
            # Print the collection name for each sample record
            print(row.get('name', 'No Name'))

        # Close the database connection
        self.db.close()


if __name__ == '__main__':
    etl = OpenSeaETL()
    etl.run_pipeline()