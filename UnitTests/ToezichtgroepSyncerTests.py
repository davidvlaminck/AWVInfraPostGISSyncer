from unittest import TestCase

from psycopg2 import connect

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from ToezichtgroepSyncer import ToezichtgroepSyncer


class ToezichtgroepSyncerTests(TestCase):
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

        self.toezichtgroepen_syncer = ToezichtgroepSyncer(postGIS_connector=self.connector, emInfraImporter=self.eminfra_importer)

    def test_update_toezichtgroepen(self):
        self.setup()

        create_toezichtgroep_query = "INSERT INTO toezichtgroepen (uuid, naam, typeGroep, referentie, actief) " \
                                     "VALUES ('f07b553b-a5eb-4140-a51e-3738e51cbaa9', 'testgroep', 'intern', 'test', true)"
        select_toezichtgroep_query = "SELECT naam FROM toezichtgroepen WHERE uuid = '{uuid}'"
        count_toezichtgroep_query = "SELECT count(*) FROM toezichtgroepen"
        cursor = self.connector.connection.cursor()
        cursor.execute(create_toezichtgroep_query)

        with self.subTest('name check the first toezichtgroep created'):
            cursor.execute(select_toezichtgroep_query.replace('{uuid}', 'f07b553b-a5eb-4140-a51e-3738e51cbaa9'))
            result = cursor.fetchone()[0]
            self.assertEqual('testgroep', result)
        with self.subTest('number of toezichtgroepen before update'):
            cursor.execute(count_toezichtgroep_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        toezichtgroepen = [{'@type': 'http://purl.org/dc/terms/toezichtgroep',
                   '@id': 'https://data.awvvlaanderen.be/id/asset/005162f7-1d84-4558-b911-1f09a2e26640-cHVybDpBZ2VudA',
                   'purl:toezichtgroep.contactinfo': [
                       {'schema:ContactPoint.telefoon': '+3233666824', 'schema:ContactPoint.email': 'lvp@trafiroad.be'}],
                   'purl:toezichtgroep.naam': 'Ludovic Van Pée'},
                  {'@type': 'http://purl.org/dc/terms/toezichtgroep',
                   '@id': 'https://data.awvvlaanderen.be/id/asset/0081576c-a62d-4b33-a884-597532cfdd77-cHVybDpBZ2VudA',
                   'purl:toezichtgroep.naam': 'Frederic Crabbe', 'purl:toezichtgroep.contactinfo': [
                      {'schema:ContactPoint.email': 'frederic.crabbe@mow.vlaanderen.be',
                       'schema:ContactPoint.telefoon': '+3250248103',
                       'schema:ContactPoint.adres': {'DtcAdres.straatnaam': 'CEL SCHADE WVL'}}]},
                  {'@type': 'http://purl.org/dc/terms/toezichtgroep',
                   '@id': 'https://data.awvvlaanderen.be/id/asset/d2d0b44c-f8ba-4780-a3e7-664988a6db66',
                   'purl:toezichtgroep.naam': 'unit test changed'}]
        self.toezichtgroepen_syncer.update_toezichtgroepen(toezichtgroep_dicts=toezichtgroepen)

        with self.subTest('name check after the first toezichtgroep updated'):
            cursor.execute(select_toezichtgroep_query.replace('{uuid}', 'd2d0b44c-f8ba-4780-a3e7-664988a6db66'))
            result = cursor.fetchone()[0]
            self.assertEqual('unit test changed', result)
        with self.subTest('name check after new toezichtgroepen created'):
            cursor.execute(select_toezichtgroep_query.replace('{uuid}', '005162f7-1d84-4558-b911-1f09a2e26640'))
            result = cursor.fetchone()[0]
            self.assertEqual('Ludovic Van Pée', result)
        with self.subTest('number of toezichtgroepen after update'):
            cursor.execute(count_toezichtgroep_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)
