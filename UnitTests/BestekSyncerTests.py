from unittest import TestCase

from psycopg2 import connect

from BestekSyncer import BestekSyncer
from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager


class BestekSyncerTests(TestCase):
    def setup(self):
        settings_manager = SettingsManager(
            settings_path='/home/davidlinux/Documents/AWV/resources/settings_AwvinfraPostGISSyncer.json')
        unittest_db_settings = settings_manager.settings['databases']['unittest']

        conn = connect(host=unittest_db_settings['host'], port=unittest_db_settings['port'],
                       user=unittest_db_settings['user'], password=unittest_db_settings['password'],
                       database="postgres")
        conn.autocommit = True

        cursor = conn.cursor()
        cursor.execute('DROP database unittests;')
        cursor.execute('CREATE database unittests;')

        conn.close()

        self.connector = PostGISConnector(host=unittest_db_settings['host'], port=unittest_db_settings['port'],
                                          user=unittest_db_settings['user'], password=unittest_db_settings['password'],
                                          database="unittests")
        self.connector.set_up_tables('../setup_tables_querys.sql')

        requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
        request_handler = RequestHandler(requester)
        self.eminfra_importer = EMInfraImporter(request_handler)

        self.bestekken_syncer = BestekSyncer(postGIS_connector=self.connector, em_infra_importer=self.eminfra_importer)

    def test_update_bestekken(self):
        self.setup()

        create_bestek_query = "INSERT INTO bestekken (uuid, eDeltaDossiernummer, eDeltaBesteknummer, aannemerNaam) " \
                              "VALUES ('00e98a45-2f5f-4a01-99f8-88255b9db84b', 'dossiernummer', 'besteknummer', " \
                              "'aannemer')"
        select_bestek_query = "SELECT eDeltaDossiernummer FROM bestekken WHERE uuid = '{uuid}'"
        count_bestek_query = "SELECT count(*) FROM bestekken"
        cursor = self.connector.connection.cursor()
        cursor.execute(create_bestek_query)

        with self.subTest('dossiernummer check the first bestek created'):
            cursor.execute(select_bestek_query.replace('{uuid}', '00e98a45-2f5f-4a01-99f8-88255b9db84b'))
            result = cursor.fetchone()[0]
            self.assertEqual('dossiernummer', result)
        with self.subTest('number of bestekken before update'):
            cursor.execute(count_bestek_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        bestekken = [{
            "uuid": "00e98a45-2f5f-4a01-99f8-88255b9db84b",
            "awvId": "a9c0ab5d-734c-38c7-a3ae-d380f7efe0ad",
            "eDeltaDossiernummer": "VWT/INN/2020/011_MC001W",
            "eDeltaBesteknummer": "VWT/INN/2020/011_MC001W",
            "type": "OVERHEIDSOPDRACHT",
            "aannemerNaam": "Dynniq Belgium",
            "aannemerReferentie": "0477899895"
        },
            {
                "uuid": "014b6cc6-9b4b-42d0-9e10-77bcdfce94ce",
                "awvId": "8775871b-ee45-3aa4-88d5-bbe373dedf18",
                "eDeltaDossiernummer": "INTERN-1834",
                "eDeltaBesteknummer": "EMTG-WHG",
                "type": "INTERN",
                "aannemerNaam": "EMT WERKHUIS GENT",
                "aannemerReferentie": "WHG"
            },
            {
                "uuid": "015e46aa-6522-45bc-a570-a0565f51ad7a",
                "nummer": "1M3D8N/12/04",
                "awvId": "462e87a0-bf54-31a0-a2da-65d21b9d4e28",
                "eDeltaDossiernummer": "MDN/04-2",
                "eDeltaBesteknummer": "1M3D8N/12/04",
                "lot": "P2 : LIMB",
                "type": "OVERHEIDSOPDRACHT",
                "aannemerNaam": "V.S.E. - PAQUE - FABRICOM",
                "aannemerReferentie": "0822497246"
            }]
        self.bestekken_syncer.update_bestekken(bestekken_dicts=bestekken)

        with self.subTest('dossiernummer check after the first bestek updated'):
            cursor.execute(select_bestek_query.replace('{uuid}', '00e98a45-2f5f-4a01-99f8-88255b9db84b'))
            result = cursor.fetchone()[0]
            self.assertEqual('VWT/INN/2020/011_MC001W', result)
        with self.subTest('dossiernummer check after new bestekken created'):
            cursor.execute(select_bestek_query.replace('{uuid}', '014b6cc6-9b4b-42d0-9e10-77bcdfce94ce'))
            result = cursor.fetchone()[0]
            self.assertEqual('INTERN-1834', result)
        with self.subTest('number of bestekken after update'):
            cursor.execute(count_bestek_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)
