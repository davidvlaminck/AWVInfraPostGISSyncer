import contextlib
import itertools
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from itertools import chain, islice
from zoneinfo import ZoneInfo

from pipeline_state import PipelineState

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


def _wait_for_resume(db_path: str, timeout_hours: int, color: str = "", stop_event=None) -> bool:
    logging.info(f"{color}wachten op postgis_sync resuming signaal (max. {timeout_hours} uur)")
    start_time = time.time()
    while True:
        if stop_event is not None and stop_event.is_set():
            return False
        try:
            pipeline = PipelineState(db_path)
            state = pipeline.get()
            if state and state.get("phase") == "postgis_sync_resuming" and state.get("status") == "running":
                logging.info(f"{color}pipeline resume signal received, resuming sync")
                return True
        except (sqlite3.Error, OSError):
            pass

        now_local = now_in_brussels()
        if now_local.hour >= 8:
            logging.warning(f"{color}08:00 local time reached, forcing resume")
            return False

        if time.time() - start_time >= timeout_hours * 3600:
            logging.warning(f"{color}{timeout_hours}h resume-timeout reached, forcing resume")
            return False

        if stop_event is not None:
            if stop_event.wait(30):
                return False
        else:
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


def is_pipeline_paused(db_path: str) -> bool:
    if not db_path:
        return False
    try:
        pipeline = PipelineState(db_path)
        state = pipeline.get()
        if state:
            return state.get("phase") in ("postgis_sync_paused", "postgis_sync_pausing")
    except (sqlite3.Error, OSError):
        pass
    return False


def update_daily_views(connector, color: str = ""):
    connection = None
    try:
        connection = connector.get_connection()
        params = connector.get_params(connection)
        last_update_views_date = params['last_update_utc_views'].date()
        today_date = now_in_brussels().date()

        if today_date <= last_update_views_date:
            return

        select_view_names_query = "select viewname from pg_catalog.pg_views where schemaname = 'asset_views'"
        with connection.cursor() as cursor:
            cursor.execute(select_view_names_query)

            for view_name in cursor.fetchall():
                view_name = view_name[0]
                logging.info(f'{color}creating fixed table for {view_name}')
                view_query = f"DROP TABLE IF EXISTS asset_daily_views.{view_name}; " \
                             f"CREATE TABLE asset_daily_views.{view_name} AS SELECT * FROM asset_views.{view_name};"
                cursor.execute(view_query)
                connection.commit()

        connector.update_params(params={'last_update_utc_views': now_in_brussels()},
                               connection=connection)
    except Exception as exc:
        logging.error(f"{color}Could not create view tables")
        logging.error(exc)
        if connection:
            connection.rollback()
    finally:
        if connection:
            connector.kill_connection(connection)
