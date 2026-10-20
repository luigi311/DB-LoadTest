from contextlib import contextmanager
from threading import Thread

from tqdm import tqdm

# Workers receive the queue through the multiprocessing pool initializer.
# Only the parent process renders the progress bar and query output.
_progress_queue = None


def set_progress_queue(progress_queue):
    global _progress_queue
    _progress_queue = progress_queue


def query_finished():
    if _progress_queue is not None:
        _progress_queue.put(("completed", None))


def write(message):
    if _progress_queue is None:
        print(message)
    else:
        _progress_queue.put(("message", str(message)))


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
            postfix={"remaining": total},
        ) as progress:

            def consume():
                while True:
                    event = progress_queue.get()
                    if event is None:
                        return
                    kind, message = event
                    if kind == "completed":
                        progress.set_postfix(
                            remaining=total - progress.n - 1, refresh=False
                        )
                        progress.update(1)
                    else:
                        tqdm.write(message)

            listener = Thread(target=consume, daemon=True)
            listener.start()
            try:
                yield
            finally:
                progress_queue.put(None)
                listener.join()
    finally:
        set_progress_queue(previous_queue)
