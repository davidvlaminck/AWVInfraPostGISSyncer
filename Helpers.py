import itertools
import logging
import sqlite3
import time
from datetime import datetime, timezone
from itertools import chain, islice
from zoneinfo import ZoneInfo

from sqlite_queue_client import enqueue_sqlite_job


BRUSSELS_TZ = ZoneInfo('Europe/Brussels')


def now_in_brussels() -> datetime:
    return datetime.now(BRUSSELS_TZ)


def turn_list_of_lists_into_string(arr: [[]]) -> str:
    return ','.join('(' + ','.join(row) + ')' for row in arr)


def peek_generator(iterable):
    try:
        first = next(iterable)
    except StopIteration:
        return None
    yield from itertools.chain([first], iterable)


def ichunked(seq, chunksize):
    """Yields items from an iterator in iterable chunks."""
    it = iter(seq)
    while True:


        try:
            yield chain([next(it)], islice(it, chunksize - 1))
        except StopIteration:
            break


def chunked(seq, chunksize):
    """Yields items from an iterator in list chunks."""
    for chunk in ichunked(seq, chunksize):
        yield list(chunk)

def construct_naampad(input_dict: dict) -> str:
    """
    Construct naampad by walking recursively in a nested "parent" dictionary, searching for the "naam" key.
    Concatenates all "naam" values, starting from the top of the nested tree.
    :param input_dict:
    :return: str
    """
    naam_list = []
    while "naam" in input_dict:
        naam_list.insert(0, input_dict["naam"]) # insert at first list index position
        if "parent" in input_dict:
            input_dict = input_dict["parent"]
        else:
            break
    return "/".join(naam_list)  # concatenate naampad, using "/" as a separator character


def handle_pipeline_pause(db_path: str, post_pause_callback=None, color: str = "") -> bool:
    if not db_path:
        return False

    try:
        conn = sqlite3.connect(db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT phase, status FROM pipeline_state WHERE id = 1"
        ).fetchone()
        conn.close()

        state = dict(row) if row else {}
        if state.get("phase") != "postgis_sync_pausing" or state.get("status") != "running":
            return False

        logging.info(f"{color}postgis_sync pausing signaal ontvangen, synchronizen stoppen")

        enqueue_sqlite_job(
            action="update_pipeline_state",
            payload={
                "phase": "postgis_sync_paused",
                "status": "completed",
                "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "message": f"{color}PostGIS-sync gepauzeerd door pipeline signal",
            },
        )

        if post_pause_callback is not None:
            logging.info(f"{color}extra views aanmaken tijdens pauze")
            post_pause_callback()

        logging.info(f"{color}wachten op postgis_sync resuming signaal (max. 3 uur)")
        start_time = time.time()
        while True:
            try:
                conn = sqlite3.connect(db_path, timeout=30)
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT phase, status FROM pipeline_state WHERE id = 1"
                ).fetchone()
                conn.close()

                state = dict(row) if row else {}
                if state.get("phase") == "postgis_sync_resuming" and state.get("status") == "running":
                    break
            except (sqlite3.Error, OSError):
                pass

            if time.time() - start_time >= 3 * 3600:
                logging.info(f"{color}3 uur gewacht zonder resume-signaal, hervat zonder status-update")
                return True

            time.sleep(30)

        enqueue_sqlite_job(
            action="update_pipeline_state",
            payload={
                "phase": "postgis_sync_running",
                "status": "running",
                "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "message": f"{color}PostGIS-sync hervat door pipeline signal",
            },
        )

        return True
    except (sqlite3.Error, OSError) as exc:
        logging.error(f"Failed to read pipeline state from SQLite: {exc}")
        return False