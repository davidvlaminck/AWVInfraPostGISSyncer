from unittest import TestCase
from unittest.mock import MagicMock

from psycopg2 import connect

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
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

        self.eminfra_importer = EMInfraImporter(MagicMock())

        self.toezichtgroepen_syncer = ToezichtgroepSyncer(postgis_connector=self.connector,
                                                          em_infra_importer=self.eminfra_importer)

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

        self.toezichtgroepen_syncer.eminfra_importer.import_toezichtgroepen_from_webservice_page_by_page = self.return_toezichtgroepen
        self.toezichtgroepen_syncer.sync_toezichtgroepen()

        with self.subTest('name check after the first toezichtgroep updated'):
            cursor.execute(select_toezichtgroep_query.replace('{uuid}', 'f07b553b-a5eb-4140-a51e-3738e51cbaa9'))
            result = cursor.fetchone()[0]
            self.assertEqual('unit test changed', result)
        with self.subTest('name check after new toezichtgroepen created'):
            cursor.execute(select_toezichtgroep_query.replace('{uuid}', 'c5b6b204-917b-4399-abb4-528496b32806'))
            result = cursor.fetchone()[0]
            self.assertEqual('LokaalBestuur', result)
        with self.subTest('number of toezichtgroepen after update'):
            cursor.execute(count_toezichtgroep_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)

    def return_toezichtgroepen(self, page_size):
        return [{
            "uuid": "f07b553b-a5eb-4140-a51e-3738e51cbaa9",
            "_type": "intern",
            "naam": "unit test changed",
            "referentie": "test",
            "actiefInterval": {
                "van": "2021-06-10"
            },
            "contactFiche": {
                "emailVoorkeur": False,
                "faxVoorkeur": False,
                "telefoons": [
                    {
                        "nummer": "+32476286163"
                    }
                ],
                "emails": [
                    {
                        "adres": "facility@vlaanderen.be"
                    },
                    {
                        "adres": "vlabelcontrole@vlaanderen.be"
                    }
                ],
                "adressen": [
                    {
                        "straat": "Vaartstraat",
                        "nummer": "16",
                        "postcode": "9300",
                        "gemeente": "Aalst",
                        "provincie": "Oost-Vlaanderen"
                    }
                ]
            }
        },
            {
                "uuid": "c5b6b204-917b-4399-abb4-528496b32806",
                "_type": "extern",
                "naam": "LokaalBestuur",
                "referentie": "LokaalBestuur",
                "omschrijving": "Een niet nader gedefinieerd lokaal bestuur zoals een politiezone of gemeente",
                "actiefInterval": {
                    "van": "2021-04-23"
                },
                "contactFiche": {
                    "emailVoorkeur": False,
                    "faxVoorkeur": False
                }
            },
            {
                "uuid": "a6319533-5824-4ad4-bf41-401d1c355d2e",
                "_type": "intern",
                "naam": "RIS",
                "referentie": "RIS",
                "actiefInterval": {
                    "van": "2020-11-13"
                },
                "contactFiche": {
                    "emailVoorkeur": False,
                    "faxVoorkeur": False,
                    "telefoons": [
                        {
                            "nummer": "+3292539471"
                        }
                    ],
                    "emails": [
                        {
                            "adres": "ris.evergem@vlaamsewaterweg.be"
                        }
                    ],
                    "adressen": [
                        {}
                    ]
                }
            }]
