import time
from datetime import datetime


class SyncTimer:
    sync_start = None
    sync_end = None

    @staticmethod
    def calculate_sync_allowed_by_time():
        start_struct = time.strptime(SyncTimer.sync_start, "%H:%M:%S")
        end_struct = time.strptime(SyncTimer.sync_end, "%H:%M:%S")
        now = datetime.utcnow().time()
        start = now.replace(hour=start_struct.tm_hour, minute=start_struct.tm_min, second=start_struct.tm_sec)
        end = now.replace(hour=end_struct.tm_hour, minute=end_struct.tm_min, second=end_struct.tm_sec)
        v = start < now < end
        return v
