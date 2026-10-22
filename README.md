# DB-LoadTest

DB-LoadTest runs concurrent copies of SQL queries against PostgreSQL, Oracle, or
Databricks and reports how long each instance takes. Use a single SQL file or a
folder of queries to compare performance under load.

## Features

- Supported databases
    - oracle
    - postgres 
    - databricks
- Test the database connection before starting query instances.
- Run multiple copies of each query across configurable worker processes.
- Use async connections for PostgreSQL and Oracle, and threads for Databricks.
- Track completed and active instances with a progress display that refreshes
  every second.
- Report successful sample counts, average, median, p95, minimum, maximum, and
  aggregated durations per query and overall.
- Export per-instance results and a companion summary CSV.

## Requirements

- Python 3.X
- Network access and credentials for your database.
- SQL files compatible with the selected database.

## Installation

Clone the repository:

```bash
git clone https://github.com/luigi311/DB-LoadTest.git
cd DB-LoadTest
```

Using uv with the project's lockfile:

```bash
uv sync --locked
uv run main.py --help
```

Alternatively, install the dependencies with pip in a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python main.py --help
```

## Usage

### PostgreSQL

Run ten copies of every query in a folder, fetching results in batches of 1,000:

```bash
python main.py --database postgres --dsn "postgresql://localhost:5432/loadtest" --user loadtest --sql_folder ./queries/postgres --instances 10 --fetch_size 1000 --csv results.csv
```

### Oracle

Run five copies of a single query:

```bash
python main.py --database oracle --dsn "localhost:1521/FREE" --user loadtest --sql_file ./queries/oracle.sql --instances 5 --fetch_size 1000
```

### Databricks

Supply the server hostname and HTTP path for your SQL warehouse or cluster:

```bash
python main.py --database databricks --server_hostname "your-workspace.cloud.databricks.com" --http_path "/sql/1.0/warehouses/your-warehouse-id" --sql_file ./queries/databricks.sql --instances 5 --processes 1 --fetch_size 1000 --csv results.csv
```

The SQL paths in these examples refer to your own files. Databricks requires
`--fetch_size` to be positive or `-1`; the default of `0` is unsupported.

### Credentials and connection preflight

PostgreSQL and Oracle prompt for a password; Databricks prompts for an access
token. Input is hidden. Passwords and tokens are not CLI arguments.

Before starting query instances, the tool opens and closes one test connection
using the selected driver and supplied credentials. If this fails, it exits with
status `1` without starting the workload or creating or overwriting CSV files.
This checks connectivity and authentication; it does not execute or validate your
SQL. Each query instance then opens its own connection and closes it when finished.

### Arguments

| Argument | Description | Default |
| --- | --- | --- |
| `--database` | `postgres`, `oracle`, or `databricks`. | `postgres` |
| `--dsn` | Database DSN; required for PostgreSQL and Oracle. | — |
| `--user` | Database username; required for PostgreSQL and Oracle. | — |
| `--server_hostname` | Server hostname; required for Databricks. | — |
| `--http_path` | SQL warehouse or cluster HTTP path; required for Databricks. | — |
| `--sql_file` | Path to one SQL file. | — |
| `--sql_folder` | Folder containing `.sql` files. | — |
| `--instances` | Concurrent copies of **each** SQL file; required. Use a positive integer. | — |
| `--processes` | Worker process count. `0` uses the CPU count; use a positive integer to set it explicitly. | `0` |
| `--fetch_size` | Result-fetching mode: `0`, `-1`, or a positive batch size; see below. | `0` |
| `--print` | Print fetched result rows. | Disabled |
| `--prefix` | Text prepended to each query on its own line to identify the workload in the database. | `/*DB-LoadTest*/` |
| `--csv PATH` | Write per-instance results to `PATH` and a companion summary beside it. | No export |
| `-h`, `--help` | Show command-line help. | — |

Choose `--sql_file` or `--sql_folder`. If both are supplied, the folder takes
precedence. Folder loading includes only immediate files whose names end in
`.sql`; it does not recurse into subfolders. Each file's contents are submitted
as one query, without splitting SQL scripts into individual statements.

### Instances and processes

The total number of query executions is:

```text
number of SQL files × --instances
```

For example, three files with `--instances 5` start 15 query instances.
`--processes` distributes those same 15 instances across worker processes; it
does not limit the number of concurrent query connections. Empty worker buckets
are discarded, so the effective process count never exceeds the instance total.
With one effective worker, execution runs in the main process.

Within each process, PostgreSQL and Oracle run instances concurrently using
asyncio. Databricks uses one thread per instance assigned to that process, so
`--processes 1` can still show many instances executing at once. The displayed
phase describes the client's current operation, rather than the database's
internal execution or queue state.

### Fetching and printing results

| `--fetch_size` | Behavior |
| --- | --- |
| `0` | PostgreSQL advances the cursor without returning row batches to Python; Oracle scrolls to the last row. Database work still occurs. Unsupported for Databricks. |
| Positive integer | Fetch rows in batches of this size until the result is exhausted. |
| `-1` | Fetch all rows with Oracle and Databricks, or use very large batches with PostgreSQL. This can consume substantial memory. |

`--print` prints rows only when they are fetched into Python (`--fetch_size > 0`
or `-1`). Printing happens inside the measured operation and can increase its
duration. Batched fetches also emit a row-count heartbeat every five batches.

## Progress and timing

The `Queries` progress bar shows completed instances out of the total, elapsed
time, completion rate, active instances, and the remaining count. Both successful
and failed instances advance the bar when they finish. It refreshes every second
even during slow queries and deliberately omits an ETA.

Interactive terminals show active instances below the bar, including the SQL
filename, instance number, client phase (`connecting`, `executing`, `fetching`,
or `closing`), and elapsed time. Instance numbers are assigned per SQL filename
as instances start. The display shows up to ten of the oldest active instances,
depending on terminal height, and counts any additional instances. Finished and
failed instances leave the list automatically. When progress output is redirected,
the progress line includes the oldest active instance instead of a multiline
display. Progress is written to stderr.

Per-instance durations include connecting, executing, fetching, and closing the
connection. The preflight connection is excluded. Console timing summaries use
only successful instances and include:

- Sample count, average, median, p95, minimum, and maximum.
- Aggregated duration: the sum of successful instance durations. Concurrent work
  means this can exceed the run's walltime.
- Overall statistics calculated from all successful instances together.

P95 uses linear interpolation between sorted samples at position
`0.95 * (sample_count - 1)`. With one successful sample, median and p95 both equal
that sample's duration.

`Walltime` measures the workload run, including worker startup and reporting,
but excluding the connection preflight and CSV writing. It differs from the sum
of instance durations because instances run concurrently.

## CSV export

Add `--csv results.csv` to write **both** `results.csv` and
`results_summary.csv`. These contain timings and execution status, rather than
the data rows returned by your SQL.

The summary filename appends `_summary.csv` to the output filename's stem in the
same directory: `reports/run.csv` produces `reports/run_summary.csv`. The parent
directory must already exist. Both files are opened after a successful preflight,
overwriting existing files, and populated when execution exits.

### Per-instance CSV

The columns, in order, are:

```csv
run_started_at,database,sql_file,instance,status,duration_seconds,error,processes,instances_per_query,fetch_size
```

Each reported instance has a row, sorted by SQL filename and instance number.
Instance numbers match the live display. `status` is `success`, `error`, or
`cancelled`; `error` contains the reported error message when applicable.
`duration_seconds` records the measured duration even for failed instances.

### Summary CSV

The columns, in order, are:

```csv
run_started_at,database,scope,sql_file,sample_count,aggregated_seconds,average_seconds,median_seconds,p95_seconds,minimum_seconds,maximum_seconds,walltime_seconds,processes,instances_per_query,fetch_size
```

The summary has one row per SQL file with reported results (`scope=query`), sorted
by filename, followed by a final `Overall` row (`scope=overall`). Statistics use
only successful instances, matching the console summary. Files with reported
results but no successes have zero samples, zero aggregated duration, and blank
average, median, p95, minimum, and maximum values. `walltime_seconds` is populated
only on the overall row.

In both files, `run_started_at` is a UTC timestamp, `processes` is the effective
worker count, and `instances_per_query` and `fetch_size` record the run settings.
Durations are in seconds with six decimal places.

## Contributing

Contributions are welcome. Open an issue or submit a pull request for improvements
or bug fixes.

## License

See [LICENSE](LICENSE) for the GNU General Public License, version 3.
