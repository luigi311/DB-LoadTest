# DB-LoadTest

db_loadtest is a tool designed to test the load and performance of SQL queries on databases. It executes SQL queries concurrently and measures the execution time for each instance, providing insights into the database's performance under load.

## Features

  - Supported databases
    - oracle
    - postgres 
  - Execute SQL queries concurrently 
  - Measure and report execution times for each instance
  - Fetch no rows or in batches or all at once
  - Run a single query or a folder of queries

## Requirements

  - Python 3.7+
  - Your database credentials

## Installation

  Clone the repository:

  ```bash
git clone https://github.com/yourusername/db_loadtest.git
cd db_loadtest
  ```
  Install the required Python packages:
    
  ```bash
pip install -r requirements.txt
  ```

## Usage

  ```bash
python main.py --dsn <DSN> --user <USERNAME> --sql_file <SQL_FILE_PATH> --instances <NUM_INSTANCES> [--fetch_size <FETCH_SIZE>] [--database <DATABASE_TYPE>]
  ```

### Arguments

    --dsn: Data Source Name (DSN) for the database.
    --user: Username for the database.
    --sql_file: Path to the SQL file.
    --sql_folder: Path to the folder containing multiple SQL files.
    --instances: Number of parallel instances to run.
    --fetch_size (optional): Number of rows to fetch per batch. Default is 0 (no fetch). Use -1 to fetch all rows at once.
    --database (optional): Database type (postgres or oracle). Default is postgres.

### Example
  ```bash
python main.py --dsn "localhost:1521/FREE" --user "system" --sql_file .\oracle.sql --instances 5 --database "oracle" 
  ```

  ```bash
python main.py --dsn "postgresql://localhost:5432/loadtest" --user "loadtest" --sql_folder .\postgres --instances 10 --database "postgres"
  ```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.
License
