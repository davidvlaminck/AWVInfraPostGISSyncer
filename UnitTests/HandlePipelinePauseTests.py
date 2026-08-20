import os
import sqlite3
import tempfile
from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch
from types import SimpleNamespace

from zoneinfo import ZoneInfo

from Helpers import (
    _is_past_time,
    _time_string_to_seconds,
    _has_paused_today,
    _mark_paused_today,
    is_pipeline_paused,
    update_daily_views,
)
from SyncTimer import SyncTimer


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


class IsPipelinePausedTests(TestCase):
    def test_returns_false_when_no_db_path(self):
        result = is_pipeline_paused(db_path=None)
        self.assertFalse(result)

    def test_returns_false_when_db_path_empty(self):
        result = is_pipeline_paused(db_path='')
        self.assertFalse(result)

    @patch('Helpers.sqlite3.connect')
    def test_returns_true_when_phase_is_paused(self, mock_connect):
        conn = MagicMock()
        conn.row_factory = sqlite3.Row
        cursor = MagicMock()
        cursor.fetchone.return_value = {'phase': 'postgis_sync_paused', 'status': 'completed'}
        conn.execute.return_value = cursor
        mock_connect.return_value = conn

        result = is_pipeline_paused(db_path='/tmp/fake.db')
        self.assertTrue(result)

    @patch('Helpers.sqlite3.connect')
    def test_returns_false_when_phase_is_running(self, mock_connect):
        conn = MagicMock()
        conn.row_factory = sqlite3.Row
        cursor = MagicMock()
        cursor.fetchone.return_value = {'phase': 'postgis_sync_running', 'status': 'running'}
        conn.execute.return_value = cursor
        mock_connect.return_value = conn

        result = is_pipeline_paused(db_path='/tmp/fake.db')
        self.assertFalse(result)

    @patch('Helpers.sqlite3.connect')
    def test_returns_false_when_no_row(self, mock_connect):
        conn = MagicMock()
        conn.row_factory = sqlite3.Row
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        conn.execute.return_value = cursor
        mock_connect.return_value = conn

        result = is_pipeline_paused(db_path='/tmp/fake.db')
        self.assertFalse(result)

    @patch('Helpers.sqlite3.connect')
    def test_returns_false_on_sqlite_error(self, mock_connect):
        mock_connect.side_effect = sqlite3.Error("test error")
        result = is_pipeline_paused(db_path='/tmp/fake.db')
        self.assertFalse(result)


class DailyGuardTests(TestCase):
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


class UpdateDailyViewsTests(TestCase):
    def test_returns_early_when_already_updated_today(self):
        connector = MagicMock()
        connector.get_params.return_value = {
            'last_update_utc_views': datetime(2024, 1, 2, 0, 0, tzinfo=ZoneInfo('Europe/Brussels'))
        }
        connector.update_params = MagicMock()

        with patch('Helpers.now_in_brussels', return_value=datetime(2024, 1, 2, 0, 30, tzinfo=ZoneInfo('Europe/Brussels'))):
            update_daily_views(connector, color='')

        connector.get_params.assert_called_once()
        connector.update_params.assert_not_called()

    def test_creates_view_tables_when_not_updated_today(self):
        connector = MagicMock()
        connection = MagicMock()
        connector.get_connection.return_value = connection
        connector.get_params.return_value = {
            'last_update_utc_views': datetime(2024, 1, 1, 0, 0, tzinfo=ZoneInfo('Europe/Brussels'))
        }
        connector.update_params = MagicMock()

        cursor = MagicMock()
        cursor.fetchall.return_value = [('view1',), ('view2',)]
        connection.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        connection.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch('Helpers.now_in_brussels', return_value=datetime(2024, 1, 2, 0, 30, tzinfo=ZoneInfo('Europe/Brussels'))):
            update_daily_views(connector, color='')

        self.assertEqual(cursor.execute.call_count, 3)
        connector.update_params.assert_called_once()
        connector.kill_connection.assert_called_with(connection)

    def test_handles_error_gracefully(self):
        connector = MagicMock()
        connection = MagicMock()
        connector.get_connection.return_value = connection
        connector.get_params.return_value = {
            'last_update_utc_views': datetime(2024, 1, 1, 0, 0, tzinfo=ZoneInfo('Europe/Brussels'))
        }
        connection.cursor.side_effect = Exception("DB error")

        with patch('Helpers.now_in_brussels', return_value=datetime(2024, 1, 2, 0, 30, tzinfo=ZoneInfo('Europe/Brussels'))):
            update_daily_views(connector, color='')

        connection.rollback.assert_called_once()
        connector.kill_connection.assert_called_with(connection)


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

    @patch('AgentSyncer.is_pipeline_paused', return_value=False)
    @patch('AgentSyncer.time.sleep')
    @patch.object(SyncTimer, 'calculate_sync_paused_by_time', side_effect=[True, False])
    def test_pause_window_waits_then_syncs(self, mock_calc, mock_sleep, mock_ipp):
        syncer = self._make_agent_syncer()
        syncer.sync(connection=MagicMock(), stop_when_fully_synced=True)

        mock_sleep.assert_any_call(300)
        syncer.events_collector.collect_starting_from_page.assert_called_once()
        syncer.events_processor.process_events.assert_not_called()
        self.assertEqual(mock_ipp.call_count, 2)

    @patch('AgentSyncer.is_pipeline_paused', return_value=False)
    @patch('AgentSyncer.time.sleep')
    @patch.object(SyncTimer, 'calculate_sync_paused_by_time', return_value=False)
    def test_not_in_window_proceeds_to_sync(self, mock_calc, mock_sleep, mock_ipp):
        syncer = self._make_agent_syncer()
        syncer.sync(connection=MagicMock(), stop_when_fully_synced=True)
        mock_sleep.assert_not_called()
        syncer.events_collector.collect_starting_from_page.assert_called_once()

    @patch('AgentSyncer.is_pipeline_paused', side_effect=[True, False, False])
    @patch('AgentSyncer.time.sleep')
    @patch.object(SyncTimer, 'calculate_sync_paused_by_time', side_effect=[True, False])
    def test_pause_then_resume_proceeds_to_sync(self, mock_calc, mock_sleep, mock_ipp):
        syncer = self._make_agent_syncer()
        syncer.sync(connection=MagicMock(), stop_when_fully_synced=True)
        self.assertEqual(mock_ipp.call_count, 3)
        mock_sleep.assert_any_call(60)
        mock_sleep.assert_any_call(300)
        syncer.events_collector.collect_starting_from_page.assert_called_once()
