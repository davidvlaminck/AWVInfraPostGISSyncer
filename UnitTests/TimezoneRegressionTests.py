from datetime import datetime
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from AgentSyncer import AgentSyncer
from AssetSyncer import AssetSyncer
from Helpers import BRUSSELS_TZ
from PostGISConnector import PostGISConnector
from SyncTimer import SyncTimer


class TimezoneRegressionTests(TestCase):
    def test_agent_sync_finish_timestamp_uses_brussels_timezone(self):
        connector = MagicMock()
        connector.get_params.return_value = {
            'page_agents': 1,
            'event_uuid_agents': 'event-uuid',
            'pagesize': 100,
        }

        syncer = AgentSyncer(postgis_connector=connector, eminfra_importer=MagicMock())
        syncer.events_collector.collect_starting_from_page = MagicMock(
            return_value=SimpleNamespace(event_dict={'agents': []})
        )
        syncer.events_processor.process_events = MagicMock()

        with patch.object(SyncTimer, 'calculate_sync_paused_by_time', return_value=False):
            syncer.sync(connection=MagicMock(), stop_when_fully_synced=True)

        timestamp = connector.update_params.call_args.kwargs['params']['last_update_utc_agents']
        self.assertEqual(BRUSSELS_TZ, timestamp.tzinfo)
        self.assertEqual('Europe/Brussels', timestamp.tzinfo.key)

    def test_sync_timer_uses_brussels_time_and_handles_midnight_crossing(self):
        original_start = SyncTimer.sync_start
        original_end = SyncTimer.sync_end
        try:
            SyncTimer.sync_start = '22:00:00'
            SyncTimer.sync_end = '06:00:00'

            with patch('SyncTimer.now_in_brussels', return_value=datetime(2024, 1, 1, 23, 30, tzinfo=ZoneInfo('Europe/Brussels'))):
                self.assertTrue(SyncTimer.calculate_sync_paused_by_time())

            with patch('SyncTimer.now_in_brussels', return_value=datetime(2024, 1, 1, 7, 0, tzinfo=ZoneInfo('Europe/Brussels'))):
                self.assertFalse(SyncTimer.calculate_sync_paused_by_time())

            SyncTimer.sync_start = '08:00:00'
            SyncTimer.sync_end = '18:00:00'

            with patch('SyncTimer.now_in_brussels', return_value=datetime(2024, 1, 1, 12, 0, tzinfo=ZoneInfo('Europe/Brussels'))):
                self.assertTrue(SyncTimer.calculate_sync_paused_by_time())
        finally:
            SyncTimer.sync_start = original_start
            SyncTimer.sync_end = original_end

    def test_asset_view_refresh_uses_brussels_date_boundary(self):
        connector = MagicMock()
        connector.get_params.return_value = {
            'last_update_utc_views': datetime(2024, 1, 1, 0, 0, tzinfo=ZoneInfo('Europe/Brussels'))
        }
        connector.update_params = MagicMock()

        syncer = AssetSyncer(postgis_connector=connector, eminfra_importer=MagicMock())
        connection = MagicMock()

        with patch('AssetSyncer.now_in_brussels', return_value=datetime(2024, 1, 1, 0, 30, tzinfo=ZoneInfo('Europe/Brussels'))):
            syncer.update_view_tables(connection=connection, color='')

        connector.update_params.assert_not_called()
        connection.cursor.assert_not_called()

    def test_timestamp_values_are_converted_to_brussels_when_read_from_db(self):
        connector = type('ConnectorStub', (), {})()
        connector.param_type_map = {
            'last_update_utc_agents': 'timestamp'
        }

        params = {}
        PostGISConnector.add_params_entry(
            connector,
            params_dict=params,
            raw_param_record=('last_update_utc_agents', None, None, None, datetime(2024, 1, 1, 12, 0, tzinfo=ZoneInfo('UTC')))
        )

        self.assertEqual('Europe/Brussels', params['last_update_utc_agents'].tzinfo.key)
        self.assertEqual(datetime(2024, 1, 1, 13, 0, tzinfo=BRUSSELS_TZ), params['last_update_utc_agents'])

