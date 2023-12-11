from unittest import TestCase
from unittest.mock import MagicMock

from psycopg2 import connect

from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.SchadebeheerderGewijzigdProcessor import SchadebeheerderGewijzigdProcessor
from Exceptions.BeheerderMissingError import BeheerderMissingError
from PostGISConnector import PostGISConnector
from SettingsManager import SettingsManager


class SchadebeheerderGewijzigdProcessorTests(TestCase):
    def setup(self):


        self.connector = PostGISConnector(host=unittest_db_settings['host'], port=unittest_db_settings['port'],
                                          user=unittest_db_settings['user'], password=unittest_db_settings['password'],
                                          database="unittests")
        self.connector.set_up_tables('../setup_tables_querys.sql')

        self.eminfra_importer = EMInfraImporter(MagicMock())

    def test_update_schadebeheerder(self):
        self.setup()

        cursor = self.connector.connection.cursor()
        self.set_up_schadebeheerders(cursor)
        self.set_up_assets(cursor)

        processor = SchadebeheerderGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer)
        processor.eminfra_importer.import_assets_from_webservice_by_uuids = self.return_assets

        select_beheerder_query = "SELECT schadebeheerder FROM assets WHERE uuid = '{uuid}'"
        cursor = self.connector.connection.cursor()

        with self.subTest('check schadebeheerder of first asset'):
            cursor.execute(select_beheerder_query.replace('{uuid}', '00000000-0000-1000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual(None, result)
        with self.subTest('check asset3 before schadebeheerder updated'):
            cursor.execute(
                select_beheerder_query.replace('{uuid}', '00000000-0000-3000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('00000000-0000-0000-1000-000000000000', result)

        processor.process(uuids=['00000000-0000-1000-0000-000000000000', '00000000-0000-2000-0000-000000000000',
                                 '00000000-0000-3000-0000-000000000000'])

        with self.subTest('check asset1 after schadebeheerder updated'):
            cursor.execute(
                select_beheerder_query.replace('{uuid}', '00000000-0000-1000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('00000000-0000-0000-1000-000000000000', result)
        with self.subTest('check asset2 after schadebeheerder updated'):
            cursor.execute(
                select_beheerder_query.replace('{uuid}', '00000000-0000-2000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual(None, result)
        with self.subTest('check asset3 after schadebeheerder updated'):
            cursor.execute(
                select_beheerder_query.replace('{uuid}', '00000000-0000-3000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('00000000-0000-0000-2000-000000000000', result)

    def test_missing_beheerder(self):
        self.setup()

        cursor = self.connector.connection.cursor()
        self.set_up_one_asset(cursor)

        self.connector.commit_transaction()

        processor = SchadebeheerderGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer)
        processor.eminfra_importer.import_assets_from_webservice_by_uuids = self.return_assets

        with self.assertRaises(BeheerderMissingError):
            processor.process(['00000000-0000-1000-0000-000000000000'])
        self.connector.connection.close()

    def set_up_schadebeheerders(self, cursor):
        cursor.execute("""
        INSERT INTO beheerders (uuid, naam, referentie, actief) 
        VALUES ('00000000-0000-0000-1000-000000000000', 'groep1', 'groep1', TRUE),
            ('00000000-0000-0000-2000-000000000000', 'groep2', 'groep2', TRUE)""")

    def set_up_one_asset(self, cursor):
        cursor.execute("""
        INSERT INTO assettypes (uuid, naam, label, uri, actief) 
        VALUES ('00000000-1000-0000-0000-000000000000', 'type1', 'type1', 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#type1', TRUE)""")

        cursor.execute("""
        INSERT INTO assets (uuid, naam, assettype, schadebeheerder, actief) 
        VALUES ('00000000-0000-1000-0000-000000000000', 'asset1',
            '00000000-1000-0000-0000-000000000000', NULL, TRUE)""")

    def set_up_assets(self, cursor):
        cursor.execute("""
        INSERT INTO assettypes (uuid, naam, label, uri, actief) 
        VALUES ('00000000-1000-0000-0000-000000000000', 'type1', 'type1', 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#type1', TRUE)""")

        cursor.execute("""
        INSERT INTO assets (uuid, naam, assettype, schadebeheerder, actief) 
        VALUES ('00000000-0000-1000-0000-000000000000','asset1','00000000-1000-0000-0000-000000000000',NULL,TRUE),
        ('00000000-0000-2000-0000-000000000000', 'asset2', '00000000-1000-0000-0000-000000000000', 
            '00000000-0000-0000-1000-000000000000', TRUE),
        ('00000000-0000-3000-0000-000000000000', 'asset3', '00000000-1000-0000-0000-000000000000', 
            '00000000-0000-0000-1000-000000000000', TRUE)""")

    def return_assets(self, asset_uuids):
        return [{
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-1000-0000-000000000000-bGdjOmluc3RhbGxhdGllI0thc3Q",
            "NaampadObject.naampad": "asset1",
            "tz:Schadebeheerder.schadebeheerder": {
                "tz:DtcBeheerder.naam": "groep1",
                "tz:DtcBeheerder.referentie": "groep1"
            },
            "AIMDBStatus.isActief": True,
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "00000000-0000-1000-0000-000000000000-bGdjOmluc3RhbGxhdGllI0thc3Q",
                "DtcIdentificator.toegekendDoor": "AWV"
            }
        }, {
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-2000-0000-000000000000-bGdjOmluc3RhbGxhdGllI0thc3Q",
            "NaampadObject.naampad": "asset2",
            "AIMDBStatus.isActief": True,
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "00000000-0000-2000-0000-000000000000-bGdjOmluc3RhbGxhdGllI0thc3Q",
                "DtcIdentificator.toegekendDoor": "AWV"
            }
        },{
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-3000-0000-000000000000-bGdjOmluc3RhbGxhdGllI0thc3Q",
            "NaampadObject.naampad": "asset3",
            "tz:Schadebeheerder.schadebeheerder": {
                "tz:DtcBeheerder.naam": "groep2",
                "tz:DtcBeheerder.referentie": "groep2"
            },
            "AIMDBStatus.isActief": True,
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "00000000-0000-3000-0000-000000000000-bGdjOmluc3RhbGxhdGllI0thc3Q",
                "DtcIdentificator.toegekendDoor": "AWV"
            }
        }]
