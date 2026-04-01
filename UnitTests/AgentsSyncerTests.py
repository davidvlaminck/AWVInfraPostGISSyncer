from datetime import datetime
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from zoneinfo import ZoneInfo

from psycopg2 import connect

from AgentSyncer import AgentSyncer
from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from SyncTimer import SyncTimer


class AgentSyncerTests(TestCase):
    def setup(self):


        self.connector = PostGISConnector(host=unittest_db_settings['host'], port=unittest_db_settings['port'],
                                          user=unittest_db_settings['user'], password=unittest_db_settings['password'],
                                          database="unittests")
        self.connector.set_up_tables('../setup_tables_querys.sql')

        requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
        request_handler = RequestHandler(requester)
        self.eminfra_importer = EMInfraImporter(request_handler)

        self.agents_syncer = AgentSyncer(postgis_connector=self.connector, eminfra_importer=self.eminfra_importer)

    def test_update_agents(self):
        self.setup()

        create_agent_query = "INSERT INTO agents (uuid, naam, actief) VALUES ('d2d0b44c-f8ba-4780-a3e7-664988a6db66', 'unit test', true)"
        select_agent_query = "SELECT naam FROM agents WHERE uuid = '{uuid}'"
        count_agent_query = "SELECT count(*) FROM agents"
        cursor = self.connector.connection.cursor()
        cursor.execute(create_agent_query)

        with self.subTest('name check the first agent created'):
            cursor.execute(select_agent_query.replace('{uuid}', 'd2d0b44c-f8ba-4780-a3e7-664988a6db66'))
            result = cursor.fetchone()[0]
            self.assertEqual('unit test', result)
        with self.subTest('number of agents before update'):
            cursor.execute(count_agent_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        agents = [{'@type': 'http://purl.org/dc/terms/Agent',
                   '@id': 'https://data.awvvlaanderen.be/id/asset/005162f7-1d84-4558-b911-1f09a2e26640-cHVybDpBZ2VudA',
                   'purl:Agent.contactinfo': [
                       {'schema:ContactPoint.telefoon': '+3233666824', 'schema:ContactPoint.email': 'lvp@trafiroad.be'}],
                   'purl:Agent.naam': 'Ludovic Van Pée'},
                  {'@type': 'http://purl.org/dc/terms/Agent',
                   '@id': 'https://data.awvvlaanderen.be/id/asset/0081576c-a62d-4b33-a884-597532cfdd77-cHVybDpBZ2VudA',
                   'purl:Agent.naam': 'Frederic Crabbe', 'purl:Agent.contactinfo': [
                      {'schema:ContactPoint.email': 'frederic.crabbe@mow.vlaanderen.be',
                       'schema:ContactPoint.telefoon': '+3250248103',
                       'schema:ContactPoint.adres': {'DtcAdres.straatnaam': 'CEL SCHADE WVL'}}]},
                  {'@type': 'http://purl.org/dc/terms/Agent',
                   '@id': 'https://data.awvvlaanderen.be/id/asset/d2d0b44c-f8ba-4780-a3e7-664988a6db66',
                   'purl:Agent.naam': 'unit test changed'}]
        self.agents_syncer.update_objects(object_dicts=agents)

        with self.subTest('name check after the first agent updated'):
            cursor.execute(select_agent_query.replace('{uuid}', 'd2d0b44c-f8ba-4780-a3e7-664988a6db66'))
            result = cursor.fetchone()[0]
            self.assertEqual('unit test changed', result)
        with self.subTest('name check after new agents created'):
            cursor.execute(select_agent_query.replace('{uuid}', '005162f7-1d84-4558-b911-1f09a2e26640'))
            result = cursor.fetchone()[0]
            self.assertEqual('Ludovic Van Pée', result)
        with self.subTest('number of agents after update'):
            cursor.execute(count_agent_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)

    def test_sync_when_fully_synced_uses_brussels_timezone_for_last_update(self):
        connector = MagicMock()
        connector.get_params.return_value = {
            'page_agents': 1,
            'event_uuid_agents': 'event-uuid',
            'pagesize': 100,
        }

        eminfra_importer = MagicMock()
        syncer = AgentSyncer(postgis_connector=connector, eminfra_importer=eminfra_importer)
        syncer.events_collector.collect_starting_from_page = MagicMock(
            return_value=SimpleNamespace(event_dict={'agents': []})
        )
        syncer.events_processor.process_events = MagicMock()

        with patch.object(SyncTimer, 'calculate_sync_allowed_by_time', return_value=True):
            syncer.sync(connection=MagicMock(), stop_when_fully_synced=True)

        timestamp = connector.update_params.call_args.kwargs['params']['last_update_utc_agents']
        self.assertEqual(ZoneInfo('Europe/Brussels'), timestamp.tzinfo)
        self.assertEqual('Europe/Brussels', timestamp.tzinfo.key)

