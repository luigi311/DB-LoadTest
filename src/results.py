import csv
from dataclasses import asdict, dataclass
from typing import TextIO


@dataclass
class QueryOutcome:
    query_id: str | None
    duration_seconds: float = 0
    status: str = "success"
    error: str = ""


@dataclass
class QueryResult:
    sql_file: str
    instance: int
    status: str
    duration_seconds: float
    error: str = ""


CSV_FIELDS = (
    "run_started_at",
    "database",
    "sql_file",
    "instance",
    "status",
    "duration_seconds",
    "error",
    "processes",
    "instances_per_query",
    "fetch_size",
)


def write_results_csv(output: TextIO, results: list[QueryResult], settings: dict) -> None:
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDS)
    writer.writeheader()
    for result in sorted(results, key=lambda result: (result.sql_file, result.instance)):
        row = {**settings, **asdict(result)}
        row["duration_seconds"] = f"{result.duration_seconds:.6f}"
        writer.writerow(row)
