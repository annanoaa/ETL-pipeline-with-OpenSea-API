# ETL Pipeline with OpenSea API and Custom ORM

This project implements a custom Object-Relational Mapping (ORM) system with essential CRUD operations and an ETL (Extract, Transform, Load) pipeline that retrieves collection data from the OpenSea API for the Ethereum chain.

## Features

### Custom ORM System
- **Database Connection and Management**  
  - Connects to SQLite (and optionally PostgreSQL) using a unified interface.  
  - Creates and deletes tables using `create_table` and `drop_table` methods.
  - Connects to existing tables via user-defined schemas.

- **Schema Management**  
  - **Add Columns:** Use `add_column` to easily add new columns.  
  - **Remove Columns:** `drop_column` is available (with the limitation that SQLite does not support it natively).  
  - **Change Data Types:** `alter_column` is available (with support limitations for SQLite).

- **Data Retrieval**  
  - Supports `SELECT` statements via the `select` method.  
  - Implements the `LIMIT` clause and `ORDER BY` clause to restrict and sort results.
  - Data filtering is supported by allowing various operators (e.g., `LIKE`, `ILIKE`, `IN`) by passing the appropriate operator in the `where` clause tuples.

- **Data Insertion**  
  - Insert single or multiple rows using the `insert` method.

- **Note on Updates and Deletes:**  
  - While the ORM currently supports table deletion as part of schema management, row-level update and delete operations have not been implemented. These can be added as needed following similar patterns used in the current methods.

### ETL Pipeline with OpenSea API
- **Data Extraction:**  
  - Uses the OpenSea Collections API endpoint (`https://api.opensea.io/api/v2/collections`) to extract collection data filtered to the Ethereum chain.
  - Handles pagination through the `next` attribute in the API response.
  - Saves the raw data into a local data lake directory (`data_lake/raw`) in JSON format.

- **Data Transformation:**  
  - The pipeline transforms and cleans the extracted data before loading it into the database.
  - Each collection is mapped to a defined schema with the following fields:  
    - `collection`: The name of the collection.  
    - `name`: The name of the collection item.  
    - `description`: A description of the collection item.  
    - `image_url`: The URL of the associated image.  
    - `owner`: The owner’s address.  
    - `twitter_username`: The Twitter username of the collection owner.  
    - `contracts`: Contract details, stored as a JSON string.

- **Data Loading:**  
  - Uses the custom ORM’s `insert` method to load transformed data into the `collections` table.

## Files

- **orm.py**  
  Implements the custom ORM with methods for connecting to a database, creating/dropping tables, managing schemas (add/drop/alter columns), inserting data, and performing SELECT queries with filtering, ordering, and limiting support.

- **etl.py**  
  Contains the ETL pipeline which:
  1. Creates the necessary table using the ORM.  
  2. Extracts data from the OpenSea API (using an API key stored in `.env` file).  
  3. Transforms the raw data into the desired schema format.  
  4. Loads the transformed data into the database.  
  5. Saves a copy of the raw data to a local data lake directory.

## How to Run

1. Ensure Python 3.x is installed.
2. Install required dependencies:
   ```bash
   pip install requests python-dotenv