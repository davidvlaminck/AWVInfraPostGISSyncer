import logging
import sqlite3
import time
from datetime import datetime, timezone


class PipelineStateClient:
    PAUSE_PHASE = "postgis_sync_pausing"
    PAUSE_STATUS = "running"
    PAUSED_PHASE = "postgis_sync_paused"
    PAUSED_STATUS = "completed"
    RESUME_PHASE = "postgis_sync_resuming"
    RESUME_STATUS = "running"
    RUNNING_PHASE = "postgis_sync_running"
    RUNNING_STATUS = "completed"

    RESUME_TIMEOUT_SECONDS = 3 * 3600
    POLL_INTERVAL_SECONDS = 30

    def __init__(self, db_path: str = None, enabled: bool = False):
        self.db_path = db_path
        self.enabled = enabled

    def is_enabled(self) -> bool:
        return self.enabled and self.db_path is not None

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def get_state(self) -> dict | None:
        if not self.is_enabled():
            return None
        try:
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT phase, status, updated_at, message FROM pipeline_state WHERE id = 1"
                ).fetchone()
                return dict(row) if row else None
        except Exception as exc:
            logging.error(f"Failed to read pipeline state from SQLite: {exc}")
            return None

    def is_pausing(self) -> bool:
        state = self.get_state()
        if not state:
            return False
        return state.get("phase") == self.PAUSE_PHASE and state.get("status") == self.PAUSE_STATUS

    def is_resuming(self) -> bool:
        state = self.get_state()
        if not state:
            return False
        return state.get("phase") == self.RESUME_PHASE and state.get("status") == self.RESUME_STATUS

    def report_paused(self, message: str = "PostGIS-sync gepauzeerd"):
        self._report_state(self.PAUSED_PHASE, self.PAUSED_STATUS, message)

    def report_running(self, message: str = "PostGIS-sync hervat"):
        self._report_state(self.RUNNING_PHASE, self.RUNNING_STATUS, message)

    def handle_pause_and_resume(self, post_pause_callback=None, color: str = "") -> bool:
        if not self.is_enabled():
            return False
        if not self.is_pausing():
            return False

        logging.info(f"{color}postgis_sync pausing signaal ontvangen, synchronizen stoppen")
        self.report_paused(message=f"{color}PostGIS-sync gepauzeerd door pipeline signal")

        if post_pause_callback is not None:
            logging.info(f"{color}extra views aanmaken tijdens pauze")
            post_pause_callback()

        logging.info(f"{color}wachten op postgis_sync resuming signaal (max. 3 uur)")
        resumed = self.wait_for_resume()

        if resumed:
            self.report_running(message=f"{color}PostGIS-sync hervat door pipeline signal")
        else:
            logging.info(f"{color}3 uur gewacht zonder resume-signaal, hervat zonder status-update")

        return True

    def _report_state(self, phase: str, status: str, message: str = ""):
        if not self.is_enabled():
            return
        try:
            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            with self._connect() as conn:
                conn.execute(
                    "UPDATE pipeline_state SET phase = ?, status = ?, updated_at = ?, message = ? WHERE id = 1",
                    (phase, status, now, message),
                )
                conn.commit()
        except Exception as exc:
            logging.error(f"Failed to write pipeline state to SQLite: {exc}")

    def wait_for_resume(self) -> bool:
        start_time = time.time()
        while True:
            if self.is_resuming():
                return True
            if time.time() - start_time >= self.RESUME_TIMEOUT_SECONDS:
                return False
            time.sleep(self.POLL_INTERVAL_SECONDS)
