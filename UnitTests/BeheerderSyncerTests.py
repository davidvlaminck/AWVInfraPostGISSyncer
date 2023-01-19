from unittest import TestCase
from unittest.mock import MagicMock

from psycopg2 import connect

from BeheerderSyncer import BeheerderSyncer
from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from SettingsManager import SettingsManager


class BeheerderSyncerTests(TestCase):
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

    def test_sync_beheerders(self):
        self.setup()

        create_beheerder_query = "INSERT INTO beheerders (uuid, naam, referentie, typeBeheerder, actief) " \
                                 "VALUES ('10000000-0000-0000-0000-000000000000', 'testnaam', 'testreferentie', 'extern', True)"
        select_beheerder_query = "SELECT naam FROM beheerders WHERE uuid = '{uuid}'"
        select_actief_beheerder_query = "SELECT actief FROM beheerders WHERE uuid = '{uuid}'"
        count_beheerders_query = "SELECT count(*) FROM beheerders"
        cursor = self.connector.connection.cursor()
        cursor.execute(create_beheerder_query)

        with self.subTest('name check the first beheerder created'):
            cursor.execute(select_beheerder_query.replace('{uuid}', '10000000-0000-0000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('testnaam', result)
        with self.subTest('number of beheerders before update'):
            cursor.execute(count_beheerders_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        self.beheerder_syncer = BeheerderSyncer(postgis_connector=self.connector,
                                                em_infra_importer=self.eminfra_importer)

        self.beheerder_syncer.em_infra_importer.import_beheerders_from_webservice_page_by_page = self.return_beheerders
        self.beheerder_syncer.fill_beheerders()

        with self.subTest('name check after the first beheerder updated'):
            cursor.execute(select_beheerder_query.replace('{uuid}', '10000000-0000-0000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('GEMEENTE', result)
        with self.subTest('actief check after the first beheerder updated'):
            cursor.execute(select_actief_beheerder_query.replace('{uuid}', '10000000-0000-0000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual(False, result)
        with self.subTest('name check after new beheerders created'):
            cursor.execute(select_beheerder_query.replace('{uuid}', '20000000-0000-0000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('District Zuid - Limburg', result)
        with self.subTest('actief check after new beheerders created (Tot in the future)'):
            cursor.execute(select_actief_beheerder_query.replace('{uuid}', '20000000-0000-0000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual(True, result)
        with self.subTest('actief check 2 after new beheerders created (no Tot)'):
            cursor.execute(select_actief_beheerder_query.replace('{uuid}', '30000000-0000-0000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual(True, result)
        with self.subTest('number of beheerders after update'):
            cursor.execute(count_beheerders_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)

    def return_beheerders(self, page_size):
        return [{
            "_type": "extern",
            "uuid": "10000000-0000-0000-0000-000000000000",
            "naam": "GEMEENTE",
            "referentie": "GEMEENTE",
            "actiefInterval": {
                "van": "2019-01-16",
                "tot": "2021-09-23"
            },
            "contactFiche": {
                "emailVoorkeur": False,
                "faxVoorkeur": False
            }
        }, {
            "_type": "district",
            "uuid": "20000000-0000-0000-0000-000000000000",
            "naam": "District Zuid - Limburg",
            "referentie": "719",
            "actiefInterval": {
                "van": "2019-01-15",
                "tot": "3019-01-15"
            },
            "contactFiche": {
                "emailVoorkeur": True,
                "faxVoorkeur": False
            },
            "code": "1MD8GZD"
        }, {
            "_type": "extern",
            "uuid": "30000000-0000-0000-0000-000000000000",
            "naam": "GEMEENTE",
            "referentie": "GEMEENTE",
            "actiefInterval": {
                "van": "2019-01-16"
            },
            "contactFiche": {
                "emailVoorkeur": False,
                "faxVoorkeur": False
            }
        }]
