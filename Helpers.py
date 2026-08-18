import contextlib
import itertools
import logging
import os
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


def _time_string_to_seconds(time_string: str) -> int:
    parsed = time.strptime(time_string, "%H:%M:%S")
    return parsed.tm_hour * 3600 + parsed.tm_min * 60 + parsed.tm_sec


def _is_past_time(target_time: str, current_time: datetime) -> bool:
    now_seconds = current_time.hour * 3600 + current_time.minute * 60 + current_time.second
    return now_seconds >= _time_string_to_seconds(target_time)


def _wait_for_resume(db_path: str, timeout_hours: int, color: str = "") -> bool:
    logging.info(f"{color}wachten op postgis_sync resuming signaal (max. {timeout_hours} uur)")
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
                logging.info(f"{color}pipeline resume signal received, resuming sync")
                return True
        except (sqlite3.Error, OSError):
            pass

        if time.time() - start_time >= timeout_hours * 3600:
            logging.warning(f"{color}{timeout_hours}h resume-timeout reached, forcing resume")
            return False

        time.sleep(30)


def _get_pause_marker_path(db_path: str) -> str:
    return db_path + ".pause_date"


def _has_paused_today(marker_path: str) -> bool:
    if not os.path.exists(marker_path):
        return False
    try:
        with open(marker_path, "r", encoding="utf-8") as f:
            last_date = f.read().strip()
        return last_date == now_in_brussels().date().isoformat()
    except (OSError, ValueError):
        return False


def _mark_paused_today(marker_path: str) -> None:
    with contextlib.suppress(OSError), open(marker_path, "w", encoding="utf-8") as f:
        f.write(now_in_brussels().date().isoformat())


def handle_pipeline_pause(db_path: str = None, post_pause_callback=None, color: str = "",
                          in_pause_window: bool = False, backup_time: str = "06:00:00") -> bool:
    if not db_path:
        return False

    marker_path = _get_pause_marker_path(db_path)
    if _has_paused_today(marker_path):
        return False

    try:
        conn = sqlite3.connect(db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT phase, status FROM pipeline_state WHERE id = 1"
        ).fetchone()
        conn.close()

        state = dict(row) if row else {}

        if state.get("phase") == "postgis_sync_paused":
            return False

        external_pause = (state.get("phase") == "postgis_sync_pausing" and
                          state.get("status") == "running")

        backup_pause = False
        if not external_pause and in_pause_window:
            if _is_past_time(backup_time, now_in_brussels()):
                backup_pause = True
                logging.info(f"{color}no pause signal received by {backup_time}, triggering backup pause flow")

        if not external_pause and not backup_pause:
            return False

        if external_pause:
            logging.info(f"{color}pipeline pause signal received, transitioning to postgis_sync_paused")

        enqueue_sqlite_job(
            action="update_pipeline_state",
            payload={
                "phase": "postgis_sync_paused",
                "status": "completed",
                "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "message": f"{color}PostGIS-sync gepauzeerd door pipeline signal",
            },
        )

        _mark_paused_today(marker_path)

        if post_pause_callback is not None:
            logging.info(f"{color}post-pause callback started (rebuilding daily views)")
            post_pause_callback()
            logging.info(f"{color}post-pause callback completed")

        resumed = _wait_for_resume(db_path, timeout_hours=4, color=color)

        if not resumed:
            logging.warning(f"{color}4h resume-timeout reached, forcing resume")

        enqueue_sqlite_job(
            action="update_pipeline_state",
            payload={
                "phase": "postgis_sync_running",
                "status": "running",
                "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "message": f"{color}PostGIS-sync hervat",
            },
        )

        return True
    except (sqlite3.Error, OSError) as exc:
        logging.error(f"Failed to read pipeline state from SQLite: {exc}")
        return False