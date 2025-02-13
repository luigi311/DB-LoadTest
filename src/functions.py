import os

from concurrent.futures import ThreadPoolExecutor
from typing import Optional


def future_thread_executor(
    args: list, threads: Optional[int], override_threads: bool = False
):
    futures_list = []
    results = []
    workers: int = os.cpu_count() * 2

    if threads:
        workers = min(threads, workers)

    if override_threads:
        workers = threads

    # If only one worker, run in main thread to avoid overhead
    if workers == 1:
        results = []
        for arg in args:
            results.append(arg[0](*arg[1:]))

        return results

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for arg in args:
            # * arg unpacks the list into actual arguments
            futures_list.append(executor.submit(*arg))

        for future in futures_list:
            try:
                result = future.result()
                results.append(result)

            except Exception as e:
                raise Exception(e)

    return results
