# DB-LoadTest

db_loadtest is a tool designed to test the load and performance of SQL queries on databases. It executes SQL queries concurrently and measures the execution time for each instance, providing insights into the database's performance under load.

## Features

  - Supported databases
    - oracle
    - postgres 
  - Execute SQL queries concurrently 
  - Measure and report execution times for each instance
  - Fetch no rows or in batches or all at once

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
    --sql_file: Path to the SQL file containing the query to be executed.
    --instances: Number of parallel instances to run.
    --fetch_size (optional): Number of rows to fetch per batch. Default is 0 (no fetch). Use -1 to fetch all rows at once.
    --database (optional): Database type (postgres or oracle). Default is postgres.

### Example
  ```bash
python main.py --dsn mydb_dsn --user myuser --sql_file query.sql --instances 5 --fetch_size 100 --database oracle
  ```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.
License
