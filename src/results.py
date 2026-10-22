import csv
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import median
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


@dataclass(frozen=True)
class DurationStats:
    sample_count: int
    median_seconds: float | None
    p95_seconds: float | None
    aggregated_seconds: float = 0
    average_seconds: float | None = None
    minimum_seconds: float | None = None
    maximum_seconds: float | None = None


def summarize_durations(durations: list[float]) -> DurationStats:
    """Compute median and p95 using inclusive linear interpolation."""
    if not durations:
        return DurationStats(0, None, None)
    ordered = sorted(durations)
    position = (len(ordered) - 1) * 0.95
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    p95 = ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
    aggregated = sum(durations)
    return DurationStats(
        sample_count=len(ordered),
        median_seconds=median(ordered),
        p95_seconds=p95,
        aggregated_seconds=aggregated,
        average_seconds=aggregated / len(ordered),
        minimum_seconds=ordered[0],
        maximum_seconds=ordered[-1],
    )


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


SUMMARY_CSV_FIELDS = (
    "run_started_at",
    "database",
    "scope",
    "sql_file",
    "sample_count",
    "aggregated_seconds",
    "average_seconds",
    "median_seconds",
    "p95_seconds",
    "minimum_seconds",
    "maximum_seconds",
    "walltime_seconds",
    "processes",
    "instances_per_query",
    "fetch_size",
)


def summary_csv_path(csv_path: str | Path) -> Path:
    path = Path(csv_path)
    return path.with_name(f"{path.stem}_summary.csv")


def write_results_csv(output: TextIO, results: list[QueryResult], settings: dict) -> None:
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDS)
    writer.writeheader()
    for result in sorted(results, key=lambda result: (result.sql_file, result.instance)):
        row = {**settings, **asdict(result)}
        row["duration_seconds"] = f"{result.duration_seconds:.6f}"
        writer.writerow(row)


def write_summary_csv(
    output: TextIO,
    results: list[QueryResult],
    settings: dict,
    walltime_seconds: float | None = None,
) -> None:
    durations_by_file: dict[str, list[float]] = {}
    for result in results:
        durations = durations_by_file.setdefault(result.sql_file, [])
        if result.status == "success":
            durations.append(result.duration_seconds)

    writer = csv.DictWriter(output, fieldnames=SUMMARY_CSV_FIELDS)
    writer.writeheader()

    def write_row(scope, file_name, durations):
        stats = summarize_durations(durations)
        row = {**settings, "scope": scope, "sql_file": file_name, **asdict(stats)}
        for key, value in asdict(stats).items():
            if key.endswith("_seconds"):
                row[key] = f"{value:.6f}" if value is not None else ""
        row["walltime_seconds"] = (
            f"{walltime_seconds:.6f}" if scope == "overall" and walltime_seconds is not None else ""
        )
        writer.writerow(row)

    all_durations = []
    for file_name in sorted(durations_by_file):
        durations = durations_by_file[file_name]
        all_durations.extend(durations)
        write_row("query", file_name, durations)
    write_row("overall", "Overall", all_durations)
