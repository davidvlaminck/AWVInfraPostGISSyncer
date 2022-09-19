from unittest import TestCase
from unittest.mock import MagicMock

from psycopg2 import connect

from EMInfraImporter import EMInfraImporter
from IdentiteitSyncer import IdentiteitSyncer
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from ToezichtgroepSyncer import ToezichtgroepSyncer


class IdentiteitSyncerTests(TestCase):
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

        self.eminfra_importer = EMInfraImporter(MagicMock())

    def test_sync_identiteiten(self):
        self.setup()

        create_identiteit_query = "INSERT INTO identiteiten (uuid, naam, voornaam, systeem, actief) " \
                                  "VALUES ('10000000-0000-0000-0000-000000000000', 'testnaam', 'testvoornaam', False, True)"
        select_identiteit_query = "SELECT naam FROM identiteiten WHERE uuid = '{uuid}'"
        count_identiteiten_query = "SELECT count(*) FROM identiteiten"
        cursor = self.connector.connection.cursor()
        cursor.execute(create_identiteit_query)

        with self.subTest('name check the first identiteit created'):
            cursor.execute(select_identiteit_query.replace('{uuid}', '10000000-0000-0000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('testnaam', result)
        with self.subTest('number of identiteiten before update'):
            cursor.execute(count_identiteiten_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        self.identiteit_syncer = IdentiteitSyncer(postgis_connector=self.connector,
                                                  em_infra_importer=self.eminfra_importer)

        self.identiteit_syncer.em_infra_importer.import_identiteiten_from_webservice_page_by_page = self.return_identiteiten
        self.identiteit_syncer.sync_identiteiten()

        with self.subTest('name check after the first identiteit updated'):
            cursor.execute(select_identiteit_query.replace('{uuid}', '10000000-0000-0000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('Vlaminck', result)
        with self.subTest('name check after new identiteiten created'):
            cursor.execute(select_identiteit_query.replace('{uuid}', '20000000-0000-0000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('Testnaam2', result)
        with self.subTest('number of identiteiten after update'):
            cursor.execute(count_identiteiten_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)

    def return_identiteiten(self, page_size):
        return [{
            "_type": "pno",
            "uuid": "10000000-0000-0000-0000-000000000000",
            "actief": True,
            "systeem": False,
            "naam": "Vlaminck",
            "gebruikersnaam": "vlaminda",
            "voornaam": "David",
            "contactFiche": {
                "emailVoorkeur": False,
                "faxVoorkeur": False
            },
            "voId": "00000000-1000-0000-0000-000000000000",
            "bron": "webidm",
            "gebruikersrechtOrganisaties": [
                "0123456789"
            ]
        }, {
            "_type": "pno",
            "uuid": "20000000-0000-0000-0000-000000000000",
            "actief": True,
            "systeem": False,
            "naam": "Testnaam2",
            "gebruikersnaam": "accountnaam2",
            "voornaam": "Voornaam2",
            "contactFiche": {
                "emailVoorkeur": False,
                "faxVoorkeur": False
            },
            "voId": "00000000-2000-0000-0000-000000000000",
            "bron": "webidm",
            "gebruikersrechtOrganisaties": [
                "0123456789"
            ]
        }, {
            "_type": "pno",
            "uuid": "30000000-0000-0000-0000-000000000000",
            "actief": True,
            "systeem": False,
            "naam": "Testnaam3",
            "gebruikersnaam": "accountnaam3",
            "voornaam": "Voornaam3",
            "contactFiche": {
                "emailVoorkeur": False,
                "faxVoorkeur": False
            },
            "voId": "00000000-3000-0000-0000-000000000000",
            "bron": "webidm",
            "gebruikersrechtOrganisaties": [
                "0123456789"
            ]
        }]
