import logging
import sqlite3
import threading
from datetime import datetime, timezone

from Helpers import (
    _get_pause_marker_path,
    _has_paused_today,
    _is_past_time,
    _mark_paused_today,
    _wait_for_resume,
    now_in_brussels,
    update_daily_views,
)
from pipeline_state import PipelineState, enqueue_sqlite_job
from SyncTimer import SyncTimer


class PauseManager:
    def __init__(self, db_path, connector, color, backup_time):
        self.db_path = db_path
        self.connector = connector
        self.color = color
        self.backup_time = backup_time
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self):
        while not self._stop_event.is_set():
            try:
                self._check_and_handle_pause()
            except Exception as exc:
                logging.error(f"PauseManager error: {exc}")
            self._stop_event.wait(30)

    def _read_state(self):
        if not self.db_path:
            return None
        try:
            pipeline = PipelineState(self.db_path)
            state = pipeline.get()
            return state if state else {}
        except (sqlite3.Error, OSError):
            return None

    def _signal_pipeline_state(self, phase: str, status: str, message: str):
        enqueue_sqlite_job(
            action="update_pipeline_state",
            payload={
                "phase": phase,
                "status": status,
                "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "message": message,
            },
        )

    def _handle_resume(self, resumed: bool):
        if self._stop_event.is_set():
            logging.error(f"{self.color}resume aborted (stop event set)")
            self._signal_pipeline_state(
                "postgis_sync_running", "failed",
                f"{self.color}PostGIS-sync hervat mislukt (stop event)",
            )
            return

        if not resumed:
            logging.warning(f"{self.color}4h resume-timeout reached, forcing resume")

        message = f"{self.color}PostGIS-sync hervat" if resumed else f"{self.color}PostGIS-sync hervat (time-out)"
        self._signal_pipeline_state("postgis_sync_running", "running", message)

        settle_seconds = 60
        logging.info(f"{self.color}wachten op sync om zich te vestigen na resume ({settle_seconds}s)")
        if self._stop_event.wait(settle_seconds):
            logging.info(f"{self.color}stop event set tijdens vestigen, completed niet gezet")
            return

        self._signal_pipeline_state(
            "postgis_sync_running", "completed",
            f"{self.color}PostGIS-sync hervat en voltooid",
        )

    def _check_and_handle_pause(self):
        state = self._read_state()
        if not state:
            return

        if state.get("phase") == "postgis_sync_paused":
            logging.info(f"{self.color}pipeline is paused, waiting for resume signal...")
            resumed = _wait_for_resume(
                self.db_path, timeout_hours=4, color=self.color,
                stop_event=self._stop_event
            )
            self._handle_resume(resumed)
            return

        marker_path = _get_pause_marker_path(self.db_path)
        if _has_paused_today(marker_path):
            return

        external_pause = (state.get("phase") == "postgis_sync_pausing" and
                          state.get("status") == "running")

        in_pause_window = SyncTimer.calculate_sync_paused_by_time()
        backup_pause = False
        if not external_pause and in_pause_window:
            if _is_past_time(self.backup_time, now_in_brussels()):
                backup_pause = True
                logging.info(f"{self.color}no pause signal received by {self.backup_time}, triggering backup pause flow")

        if not external_pause and not backup_pause:
            return

        if external_pause:
            logging.info(f"{self.color}pipeline pause signal received, transitioning to postgis_sync_paused")

        enqueue_sqlite_job(
            action="update_pipeline_state",
            payload={
                "phase": "postgis_sync_paused",
                "status": "completed",
                "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "message": f"{self.color}PostGIS-sync gepauzeerd",
            },
        )

        _mark_paused_today(marker_path)

        logging.info(f"{self.color}post-pause: rebuilding daily views")
        try:
            update_daily_views(self.connector, self.color)
        except Exception as exc:
            logging.error(f"{self.color}Could not create view tables: {exc}")

        resumed = _wait_for_resume(
            self.db_path, timeout_hours=4, color=self.color,
            stop_event=self._stop_event
        )
        self._handle_resume(resumed)
