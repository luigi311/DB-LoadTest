import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from itertools import count
from queue import Empty
from shutil import get_terminal_size
from threading import Thread
from time import monotonic

from tqdm import tqdm

PROGRESS_REFRESH_INTERVAL_SECONDS = 1
MAX_ACTIVE_QUERY_ROWS = 10

# Workers receive the queue through the multiprocessing pool initializer.
# Only the parent process renders the progress bar and query output.
_progress_queue = None
_current_query = ContextVar("current_query", default=None)
_query_ids = count(1)


def set_progress_queue(progress_queue):
    global _progress_queue
    _progress_queue = progress_queue


def query_started(file_name):
    if _progress_queue is None:
        return None
    query_id = f"{os.getpid()}:{next(_query_ids)}"
    token = _current_query.set(query_id)
    _progress_queue.put(("started", (query_id, file_name, monotonic())))
    return token


def query_phase(phase):
    query_id = _current_query.get()
    if _progress_queue is not None and query_id is not None:
        _progress_queue.put(("phase", (query_id, phase)))


def query_finished(token=None):
    if _progress_queue is not None:
        _progress_queue.put(("completed", _current_query.get()))
    if token is not None:
        _current_query.reset(token)


def write(message):
    if _progress_queue is None:
        print(message)
    else:
        _progress_queue.put(("message", str(message)))


@dataclass
class ActiveQuery:
    file_name: str
    instance: int
    started_at: float
    phase: str = "connecting"


class ActiveQueryDisplay:
    def __init__(self, progress):
        self.progress = progress
        self.active: dict[str, ActiveQuery] = {}
        self.instances: dict[str, int] = {}
        self.rows = []
        self.interactive = progress.fp.isatty()

    def started(self, query_id, file_name, started_at):
        instance = self.instances.get(file_name, 0) + 1
        self.instances[file_name] = instance
        self.active[query_id] = ActiveQuery(file_name, instance, started_at)

    def phase_changed(self, query_id, phase):
        if query_id in self.active:
            self.active[query_id].phase = phase

    def finished(self, query_id):
        self.active.pop(query_id, None)

    def update_counts(self, completed=None):
        if completed is None:
            completed = self.progress.n
        self.progress.set_postfix(
            remaining=self.progress.total - completed,
            active=len(self.active),
            refresh=False,
        )

    def render(self):
        self.update_counts()
        queries = sorted(self.active.values(), key=lambda query: query.started_at)
        now = monotonic()
        if not self.interactive:
            if queries:
                oldest = queries[0]
                elapsed = tqdm.format_interval(max(0, now - oldest.started_at))
                self.progress.set_postfix_str(
                    f"remaining={self.progress.total - self.progress.n}, "
                    f"active={len(queries)}, oldest={oldest.file_name} "
                    f"#{oldest.instance} {oldest.phase} {elapsed}",
                    refresh=False,
                )
        else:
            columns, terminal_rows = get_terminal_size()
            limit = max(1, min(MAX_ACTIVE_QUERY_ROWS, terminal_rows - 5))
            lines = []
            if queries:
                lines.append("Active queries (oldest first):")
                for query in queries[:limit]:
                    elapsed = tqdm.format_interval(max(0, now - query.started_at))
                    suffix = f" | #{query.instance} | {query.phase:<10} | {elapsed} elapsed"
                    name_width = max(1, columns - len(suffix) - 2)
                    name = query.file_name
                    if len(name) > name_width:
                        name = name[:max(0, name_width - 1)] + "…"
                    lines.append(f"  {name}{suffix}")
                if len(queries) > limit:
                    lines.append(f"  … {len(queries) - limit} more active queries")

            while len(self.rows) > len(lines):
                self.rows.pop().close()
            while len(self.rows) < len(lines):
                self.rows.append(
                    tqdm(
                        total=0,
                        position=len(self.rows) + 1,
                        bar_format="{desc}",
                        leave=False,
                        dynamic_ncols=True,
                        file=self.progress.fp,
                    )
                )
            for row, line in zip(self.rows, lines):
                row.set_description_str(line, refresh=False)
                row.refresh()
        self.progress.refresh()

    def close(self):
        while self.rows:
            self.rows.pop().close()


@contextmanager
def track_queries(total, progress_queue):
    """Render worker output and per-query progress in the parent process."""
    previous_queue = _progress_queue
    set_progress_queue(progress_queue)
    try:
        with tqdm(
            total=total,
            desc="Queries",
            unit="query",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}, {rate_fmt}{postfix}]",
            dynamic_ncols=True,
            miniters=1,
            postfix={"remaining": total, "active": 0},
        ) as progress:

            def consume():
                display = ActiveQueryDisplay(progress)
                next_refresh = monotonic()
                try:
                    while True:
                        try:
                            event = progress_queue.get(
                                timeout=max(0, next_refresh - monotonic())
                            )
                        except Empty:
                            display.render()
                            next_refresh = monotonic() + PROGRESS_REFRESH_INTERVAL_SECONDS
                            continue
                        if event is None:
                            display.render()
                            return
                        kind, message = event
                        if kind == "started":
                            display.started(*message)
                        elif kind == "phase":
                            display.phase_changed(*message)
                        elif kind == "completed":
                            display.finished(message)
                            display.update_counts(completed=progress.n + 1)
                            progress.update(1)
                        else:
                            tqdm.write(message)
                        if monotonic() >= next_refresh:
                            display.render()
                            next_refresh = monotonic() + PROGRESS_REFRESH_INTERVAL_SECONDS
                finally:
                    display.close()

            listener = Thread(target=consume, daemon=True)
            listener.start()
            try:
                yield
            finally:
                progress_queue.put(None)
                listener.join()
    finally:
        set_progress_queue(previous_queue)
