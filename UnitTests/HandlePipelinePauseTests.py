import os
import sqlite3
import tempfile
from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch
from types import SimpleNamespace

from zoneinfo import ZoneInfo

from Helpers import handle_pipeline_pause, _is_past_time, _time_string_to_seconds, _has_paused_today, _mark_paused_today
from SyncTimer import SyncTimer


def make_already_paused_row():
    return {'phase': 'postgis_sync_paused', 'status': 'completed'}


def make_paused_row():
    return {'phase': 'postgis_sync_pausing', 'status': 'running'}


def make_resumed_row():
    return {'phase': 'postgis_sync_resuming', 'status': 'running'}


def make_idle_row():
    return None


def setup_mock_connect(mock_connect, state_row, resume_row=None):
    """Configure mock sqlite3.connect to return sequential state rows."""
    call_count = [0]
    all_rows = [state_row]
    if resume_row is not None:
        all_rows.append(resume_row)

    def mock_connect_fn(*args, **kwargs):
        conn = MagicMock()
        conn.row_factory = sqlite3.Row
        cursor = MagicMock()
        idx = call_count[0] % len(all_rows)
        cursor.fetchone.return_value = all_rows[idx]
        conn.execute.return_value = cursor
        call_count[0] += 1
        return conn

    mock_connect.side_effect = mock_connect_fn


class TimeStringTests(TestCase):
    def test_time_string_to_seconds(self):
        self.assertEqual(_time_string_to_seconds("00:00:00"), 0)
        self.assertEqual(_time_string_to_seconds("01:30:00"), 5400)
        self.assertEqual(_time_string_to_seconds("06:00:00"), 21600)
        self.assertEqual(_time_string_to_seconds("23:59:59"), 86399)

    def test_is_past_time_true(self):
        now = datetime(2024, 1, 1, 7, 0, 0, tzinfo=ZoneInfo('Europe/Brussels'))
        self.assertTrue(_is_past_time("06:00:00", now))

    def test_is_past_time_false(self):
        now = datetime(2024, 1, 1, 5, 0, 0, tzinfo=ZoneInfo('Europe/Brussels'))
        self.assertFalse(_is_past_time("06:00:00", now))

    def test_sync_timer_has_backup_time_default(self):
        self.assertEqual(SyncTimer.backup_time, '06:00:00')


class HandlePipelinePauseNoDbPathTests(TestCase):
    def test_returns_false_when_no_db_path(self):
        result = handle_pipeline_pause(db_path=None, color='', in_pause_window=True)
        self.assertFalse(result)

    def test_returns_false_when_db_path_empty(self):
        result = handle_pipeline_pause(db_path='', color='', in_pause_window=True)
        self.assertFalse(result)


class HandlePipelinePauseAlreadyPausedTests(TestCase):
    @patch('Helpers.enqueue_sqlite_job')
    @patch('Helpers.sqlite3.connect')
    def test_returns_false_when_already_paused(self, mock_connect, mock_enqueue):
        setup_mock_connect(mock_connect, make_already_paused_row())

        result = handle_pipeline_pause(
            db_path='/tmp/fake.db',
            color='',
            in_pause_window=True,
            backup_time='06:00:00'
        )

        self.assertFalse(result)
        mock_enqueue.assert_not_called()


class HandlePipelinePauseDailyGuardTests(TestCase):
    @patch('Helpers.now_in_brussels')
    def test_has_paused_today_true_when_marker_has_today(self, mock_now):
        mock_now.return_value = datetime(2024, 1, 1, 12, 0, tzinfo=ZoneInfo('Europe/Brussels'))
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            f.write('2024-01-01')
            marker_path = f.name
        try:
            self.assertTrue(_has_paused_today(marker_path))
        finally:
            os.unlink(marker_path)

    @patch('Helpers.now_in_brussels')
    def test_has_paused_today_false_when_marker_has_yesterday(self, mock_now):
        mock_now.return_value = datetime(2024, 1, 1, 12, 0, tzinfo=ZoneInfo('Europe/Brussels'))
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as f:
            f.write('2023-12-31')
            marker_path = f.name
        try:
            self.assertFalse(_has_paused_today(marker_path))
        finally:
            os.unlink(marker_path)

    def test_has_paused_today_false_when_marker_missing(self):
        marker_path = '/tmp/nonexistent_pause_marker_12345'
        self.assertFalse(_has_paused_today(marker_path))

    @patch('Helpers._wait_for_resume', return_value=True)
    @patch('Helpers.enqueue_sqlite_job')
    @patch('Helpers.sqlite3.connect')
    @patch('Helpers.now_in_brussels')
    def test_returns_false_when_already_paused_today(
        self, mock_now, mock_connect, mock_enqueue, mock_wait
    ):
        mock_now.return_value = datetime(2024, 1, 1, 4, 0, 0, tzinfo=ZoneInfo('Europe/Brussels'))
        setup_mock_connect(mock_connect, make_paused_row(), make_resumed_row())

        db_path = '/tmp/fake.db'
        marker_path = db_path + '.pause_date'
        with open(marker_path, 'w', encoding='utf-8') as f:
            f.write('2024-01-01')
        try:
            result = handle_pipeline_pause(
                db_path=db_path,
                color='',
                in_pause_window=False
            )
            self.assertFalse(result)
            mock_enqueue.assert_not_called()
        finally:
            if os.path.exists(marker_path):
                os.unlink(marker_path)

    @patch('Helpers._wait_for_resume', return_value=True)
    @patch('Helpers.enqueue_sqlite_job')
    @patch('Helpers.sqlite3.connect')
    @patch('Helpers.now_in_brussels')
    def test_proceeds_when_paused_yesterday(
        self, mock_now, mock_connect, mock_enqueue, mock_wait
    ):
        mock_now.return_value = datetime(2024, 1, 2, 4, 0, 0, tzinfo=ZoneInfo('Europe/Brussels'))
        setup_mock_connect(mock_connect, make_paused_row(), make_resumed_row())

        db_path = '/tmp/fake.db'
        marker_path = db_path + '.pause_date'
        with open(marker_path, 'w', encoding='utf-8') as f:
            f.write('2024-01-01')
        try:
            result = handle_pipeline_pause(
                db_path=db_path,
                color='',
                in_pause_window=False
            )
            self.assertTrue(result)
            self.assertEqual(mock_enqueue.call_count, 2)
        finally:
            if os.path.exists(marker_path):
                os.unlink(marker_path)


class HandlePipelinePauseNoSignalTests(TestCase):
    @patch('Helpers.enqueue_sqlite_job')
    @patch('Helpers.sqlite3.connect')
    @patch('Helpers.now_in_brussels')
    def test_returns_false_when_no_signal_not_in_window(
        self, mock_now, mock_connect, mock_enqueue
    ):
        setup_mock_connect(mock_connect, make_idle_row())

        mock_now.return_value = datetime(2024, 1, 1, 10, 0, tzinfo=ZoneInfo('Europe/Brussels'))

        result = handle_pipeline_pause(
            db_path='/tmp/fake.db',
            color='',
            in_pause_window=False,
            backup_time='06:00:00'
        )

        self.assertFalse(result)
        mock_enqueue.assert_not_called()

    @patch('Helpers._wait_for_resume', return_value=True)
    @patch('Helpers.enqueue_sqlite_job')
    @patch('Helpers.sqlite3.connect')
    @patch('Helpers.now_in_brussels')
    def test_returns_false_when_in_window_but_not_past_backup_time(
        self, mock_now, mock_connect, mock_enqueue, mock_wait
    ):
        setup_mock_connect(mock_connect, make_idle_row())

        mock_now.return_value = datetime(2024, 1, 1, 3, 0, 0, tzinfo=ZoneInfo('Europe/Brussels'))

        result = handle_pipeline_pause(
            db_path='/tmp/fake.db',
            color='',
            in_pause_window=True,
            backup_time='06:00:00'
        )

        self.assertFalse(result)
        mock_enqueue.assert_not_called()
        mock_wait.assert_not_called()


class HandlePipelineBackupPauseTests(TestCase):
    def tearDown(self):
        marker_path = '/tmp/fake.db.pause_date'
        if os.path.exists(marker_path):
            os.unlink(marker_path)

    @patch('Helpers._wait_for_resume', return_value=True)
    @patch('Helpers.enqueue_sqlite_job')
    @patch('Helpers.sqlite3.connect')
    @patch('Helpers.now_in_brussels')
    def test_backup_pause_triggers_at_backup_time(
        self, mock_now, mock_connect, mock_enqueue, mock_wait
    ):
        setup_mock_connect(mock_connect, make_idle_row(), make_resumed_row())

        mock_now.return_value = datetime(2024, 1, 1, 6, 1, 0, tzinfo=ZoneInfo('Europe/Brussels'))

        callback_mock = MagicMock()
        result = handle_pipeline_pause(
            db_path='/tmp/fake.db',
            post_pause_callback=callback_mock,
            color='',
            in_pause_window=True,
            backup_time='06:00:00'
        )

        self.assertTrue(result)
        callback_mock.assert_called_once()
        self.assertEqual(mock_enqueue.call_count, 2)
        mock_wait.assert_called_once()

    @patch('Helpers._wait_for_resume', return_value=False)
    @patch('Helpers.enqueue_sqlite_job')
    @patch('Helpers.sqlite3.connect')
    @patch('Helpers.now_in_brussels')
    def test_backup_pause_timeout_forces_resume(
        self, mock_now, mock_connect, mock_enqueue, mock_wait
    ):
        setup_mock_connect(mock_connect, make_idle_row())

        mock_now.return_value = datetime(2024, 1, 1, 7, 0, 0, tzinfo=ZoneInfo('Europe/Brussels'))

        result = handle_pipeline_pause(
            db_path='/tmp/fake.db',
            color='',
            in_pause_window=True,
            backup_time='06:00:00'
        )

        self.assertTrue(result)
        self.assertEqual(mock_enqueue.call_count, 2)


class HandlePipelinePauseExternalSignalTests(TestCase):
    def tearDown(self):
        marker_path = '/tmp/fake.db.pause_date'
        if os.path.exists(marker_path):
            os.unlink(marker_path)

    @patch('Helpers._wait_for_resume', return_value=True)
    @patch('Helpers.enqueue_sqlite_job')
    @patch('Helpers.sqlite3.connect')
    def test_external_signal_triggers_pause(
        self, mock_connect, mock_enqueue, mock_wait
    ):
        setup_mock_connect(mock_connect, make_paused_row(), make_resumed_row())

        callback_mock = MagicMock()
        result = handle_pipeline_pause(
            db_path='/tmp/fake.db',
            post_pause_callback=callback_mock,
            color='',
            in_pause_window=False
        )

        self.assertTrue(result)
        callback_mock.assert_called_once()
        self.assertEqual(mock_enqueue.call_count, 2)
        mock_wait.assert_called_once_with('/tmp/fake.db', timeout_hours=4, color='')

    @patch('Helpers._wait_for_resume', return_value=False)
    @patch('Helpers.enqueue_sqlite_job')
    @patch('Helpers.sqlite3.connect')
    @patch('Helpers.now_in_brussels')
    def test_external_signal_with_backup_time_not_reached(
        self, mock_now, mock_connect, mock_enqueue, mock_wait
    ):
        setup_mock_connect(mock_connect, make_paused_row())

        mock_now.return_value = datetime(2024, 1, 1, 3, 0, 0, tzinfo=ZoneInfo('Europe/Brussels'))

        result = handle_pipeline_pause(
            db_path='/tmp/fake.db',
            color='',
            in_pause_window=True,
            backup_time='06:00:00'
        )

        self.assertTrue(result)
        self.assertEqual(mock_enqueue.call_count, 2)
        mock_wait.assert_called_once()

    @patch('Helpers._wait_for_resume', return_value=True)
    @patch('Helpers.enqueue_sqlite_job')
    @patch('Helpers.sqlite3.connect')
    def test_external_signal_without_callback(
        self, mock_connect, mock_enqueue, mock_wait
    ):
        setup_mock_connect(mock_connect, make_paused_row(), make_resumed_row())

        result = handle_pipeline_pause(
            db_path='/tmp/fake.db',
            color='',
            in_pause_window=False
        )

        self.assertTrue(result)
        self.assertEqual(mock_enqueue.call_count, 2)
        mock_wait.assert_called_once()


class SyncerPauseFlowTests(TestCase):
    def _make_agent_syncer(self):
        connector = MagicMock()
        connector.get_params.return_value = {
            'page_agents': 1,
            'event_uuid_agents': 'event-uuid',
            'pagesize': 100,
        }
        connector.update_params = MagicMock()
        eminfra_importer = MagicMock()
        from AgentSyncer import AgentSyncer
        syncer = AgentSyncer(postgis_connector=connector, eminfra_importer=eminfra_importer)
        syncer.events_collector.collect_starting_from_page = MagicMock(
            return_value=SimpleNamespace(event_dict={'agents': []})
        )
        syncer.events_processor.process_events = MagicMock()
        return syncer

    @patch('AgentSyncer.handle_pipeline_pause', return_value=False)
    @patch('AgentSyncer.time.sleep')
    @patch.object(SyncTimer, 'calculate_sync_paused_by_time', side_effect=[True, False])
    def test_pause_window_waits_then_syncs(self, mock_calc, mock_sleep, mock_hpp):
        syncer = self._make_agent_syncer()
        syncer.sync(connection=MagicMock(), stop_when_fully_synced=True)

        mock_sleep.assert_any_call(300)
        syncer.events_collector.collect_starting_from_page.assert_called_once()
        syncer.events_processor.process_events.assert_not_called()
        self.assertEqual(mock_hpp.call_args_list[0].kwargs['in_pause_window'], True)
        self.assertEqual(mock_hpp.call_args_list[0].kwargs['backup_time'], '06:00:00')
        self.assertEqual(mock_hpp.call_args_list[1].kwargs['in_pause_window'], False)

    @patch('AgentSyncer.handle_pipeline_pause', return_value=False)
    @patch('AgentSyncer.time.sleep')
    @patch.object(SyncTimer, 'calculate_sync_paused_by_time', return_value=False)
    def test_not_in_window_proceeds_to_sync(self, mock_calc, mock_sleep, mock_hpp):
        syncer = self._make_agent_syncer()
        syncer.sync(connection=MagicMock(), stop_when_fully_synced=True)
        mock_sleep.assert_not_called()
        syncer.events_collector.collect_starting_from_page.assert_called_once()

    @patch('AgentSyncer.handle_pipeline_pause', side_effect=[True, False])
    @patch('AgentSyncer.time.sleep')
    @patch.object(SyncTimer, 'calculate_sync_paused_by_time', side_effect=[True, False])
    def test_pause_then_resume_proceeds_to_sync(self, mock_calc, mock_sleep, mock_hpp):
        syncer = self._make_agent_syncer()
        syncer.sync(connection=MagicMock(), stop_when_fully_synced=True)
        self.assertEqual(mock_hpp.call_count, 2)
        mock_sleep.assert_not_called()
        syncer.events_collector.collect_starting_from_page.assert_called_once()
