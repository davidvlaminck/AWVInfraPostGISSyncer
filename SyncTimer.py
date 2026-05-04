import time

from Helpers import now_in_brussels


class SyncTimer:
    sync_start = None
    sync_end = None

    @staticmethod
    def _to_seconds(time_string: str) -> int:
        parsed = time.strptime(time_string, "%H:%M:%S")
        return parsed.tm_hour * 3600 + parsed.tm_min * 60 + parsed.tm_sec

    @staticmethod
    def calculate_sync_paused_by_time():
        now = now_in_brussels()

        start_seconds = SyncTimer._to_seconds(SyncTimer.sync_start)
        end_seconds = SyncTimer._to_seconds(SyncTimer.sync_end)
        now_seconds = now.hour * 3600 + now.minute * 60 + now.second

        if start_seconds == end_seconds:
            return False

        if start_seconds < end_seconds:
            return start_seconds <= now_seconds < end_seconds

        return now_seconds >= start_seconds or now_seconds < end_seconds

    @staticmethod
    def calculate_sync_allowed_by_time():
        return not SyncTimer.calculate_sync_paused_by_time()
