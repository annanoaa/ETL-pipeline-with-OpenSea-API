# ETL Pipeline with OpenSea API and Custom ORM

This project implements a custom Object-Relational Mapping (ORM) system with essential CRUD (Create, Read, Update, Delete) operations along with an ETL (Extract, Transform, Load) pipeline that retrieves collection data from the OpenSea API for the Ethereum chain.

## Features

### Custom ORM System
- **Database Connection and Management**  
  - Connects to SQLite (and optionally PostgreSQL) using a unified interface.  
  - Supports creating and dropping tables using the `create_table` and `drop_table` methods.
  - Connects to existing tables via user-defined schemas.

- **Schema Management**  
  - **Add Columns:** The `add_column` method enables adding new columns to an existing table.  
  - **Remove Columns:** The `drop_column` method is available (with the limitation that SQLite does not support it natively).  
  - **Change Data Types:** The `alter_column` method is provided (with support limitations for SQLite).

- **Data Retrieval and Insertion**  
  - The `select` method allows performing SELECT queries with optional filtering, ordering (`ORDER BY`), and limiting (`LIMIT`) of results.
  - Data insertion supports both single and multiple row inserts via the `insert` method.

- **Row-Level Update and Delete**  
  - **Update:** A new `update` method allows modifications of row data based on flexible WHERE conditions.
  - **Delete:** A new `delete` method enables deletion of rows based on specified conditions.

- **Data Filtering**  
  - Basic filtering is supported in the `select`, `update`, and `delete` methods by allowing operators such as IN, LIKE, and ILIKE (passed as part of the condition).  

### ETL Pipeline with OpenSea API
- **Data Extraction:**  
  - Connects to the OpenSea Collections API endpoint (`https://api.opensea.io/api/v2/collections`) and extracts collection data for the Ethereum chain.
  - Manages pagination using the `next` attribute in the API response.
  - Saves raw data in a local data lake directory (`data_lake/raw`) in JSON format.

- **Data Transformation:**  
  - Transforms and cleans the raw data to match a defined schema that includes:  
    - `collection`: The name of the collection.  
    - `name`: The name of the collection item.  
    - `description`: A description of the collection item.  
    - `image_url`: The URL of the associated image.  
    - `owner`: The owner's address.  
    - `twitter_username`: The Twitter username of the owner.  
    - `contracts`: Contract details, stored as a JSON string.

- **Data Loading:**  
  - Loads the transformed data into the `collections` table using the custom ORM’s insert method.

## Files

- **orm.py**  
  Implements the custom ORM which now includes methods for:
  - Connecting to the database,
  - Creating/dropping tables,
  - Schema management (add, drop, and alter columns),
  - Inserting and querying data,
  - **Row-Level Updates:** Update records with the new `update` method.
  - **Row-Level Deletions:** Delete records with the new `delete` method.

- **etl.py**  
  Contains the ETL pipeline that:
  1. Creates the necessary table using the ORM.
  2. Extracts data from the OpenSea API (using an API key stored in a `.env` file).
  3. Transforms the extracted data to match the schema.
  4. Inserts the transformed data into the database.
  5. Saves a raw copy of the data in the local data lake.

## How to Run

1. Ensure Python 3.x is installed.
2. Install required dependencies:
   ```bash
   pip install requests python-dotenv