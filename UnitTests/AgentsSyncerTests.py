from unittest import TestCase

from psycopg2 import connect

from AgentSyncer import AgentSyncer
from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager


class AgentSyncerTests(TestCase):
    def setup(self):
        settings_manager = SettingsManager(
            settings_path='/home/davidlinux/Documents/AWV/resources/settings_AwvinfraPostGISSyncer.json')
        unittest_db_settings = settings_manager.settings['databases']['unittest']

        conn = connect(host=unittest_db_settings['host'], port=unittest_db_settings['port'],
                       user=unittest_db_settings['user'], password=unittest_db_settings['password'],
                       database="postgres")
        conn.autocommit = True

        cursor = conn.cursor()
        cursor.execute('DROP DATABASE IF EXISTS unittests;')
        cursor.execute('CREATE DATABASE unittests;')

        conn.close()

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
