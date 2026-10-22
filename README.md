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

The `Queries` progress bar shows completed instances out of the total and a
`remaining` query count. The total includes every
SQL file multiplied by `--instances`, across all worker processes. Both successful
and failed instances advance the bar when they finish. While waiting for updates,
the bar refreshes every second.

Interactive terminals also show active query instances below the progress bar,
with the SQL filename, instance number, current client phase (connecting, executing,
fetching, or closing), and elapsed time. Instance numbers are assigned per SQL
filename as instances start. The view refreshes every second and shows up to ten
of the oldest active instances, with a count of any additional instances. Finished
and failed instances leave the list automatically. When output is redirected, the
progress line includes the oldest active instance instead of a multiline display.

### Arguments

    --dsn: Data Source Name (DSN) for the database.
    --user: Username for the database.
    --sql_file: Path to the SQL file.
    --sql_folder: Path to the folder containing multiple SQL files.
    --instances: Number of parallel instances to run.
    --fetch_size (optional): Number of rows to fetch per batch. Default is 0 (no fetch). Use -1 to fetch all rows at once.
    --database (optional): Database type (postgres or oracle). Default is postgres.
    --print (optional): Print the query results. Default is False.
    --csv (optional): Export per-instance benchmark results to the given CSV path.

### CSV export

Add `--csv results.csv` to save one row for each completed query instance:

```bash
python main.py --dsn "localhost:1521/FREE" --user "system" --sql_file oracle.sql --instances 5 --database oracle --csv results.csv
```

The columns are `run_started_at` (UTC), `database`, `sql_file`, `instance`,
`status`, `duration_seconds`, `error`, `processes`, `instances_per_query`, and
`fetch_size`. Instance numbers match the live display. Status is `success`,
`error`, or `cancelled`; durations include connecting, executing, fetching, and
closing the connection. The console timing summary continues to include only
successful queries.

The file is written when the run ends, sorted by SQL filename and instance. If
the run is interrupted, results received for completed instances are still saved.
Existing output files are overwritten, and the parent directory must exist. A
failed connection preflight does not create or overwrite the CSV file.

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
