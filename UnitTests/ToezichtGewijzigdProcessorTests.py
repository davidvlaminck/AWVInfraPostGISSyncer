from unittest import TestCase
from unittest.mock import MagicMock

from psycopg2 import connect

from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.ToezichtGewijzigdProcessor import ToezichtGewijzigdProcessor
from Exceptions.IdentiteitMissingError import IdentiteitMissingError
from Exceptions.ToezichtgroepMissingError import ToezichtgroepMissingError
from PostGISConnector import PostGISConnector
from SettingsManager import SettingsManager


class ToezichtGewijzigdProcessorTests(TestCase):
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

        self.eminfra_importer = EMInfraImporter(MagicMock())

    def test_update_toezicht(self):
        self.setup()

        cursor = self.connector.connection.cursor()
        self.set_up_identiteiten(cursor)
        self.set_up_toezichtgroepen(cursor)
        self.set_up_assets(cursor)

        processor = ToezichtGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer)
        processor.em_infra_importer.import_assets_from_webservice_by_uuids = self.return_assets

        select_toezichter_query = "SELECT toezichter FROM assets WHERE uuid = '{uuid}'"
        select_toezichtgroep_query = "SELECT toezichtgroep FROM assets WHERE uuid = '{uuid}'"
        cursor = self.connector.connection.cursor()

        with self.subTest('check toezicht of first asset'):
            cursor.execute(select_toezichter_query.replace('{uuid}', '00000000-0000-1000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual(None, result)
            cursor.execute(select_toezichtgroep_query.replace('{uuid}', '00000000-0000-1000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual(None, result)
        with self.subTest('check asset3 before toezicht updated'):
            cursor.execute(
                select_toezichter_query.replace('{uuid}', '00000000-0000-3000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('10000000-0000-0000-0000-000000000000', result)
            cursor.execute(
                select_toezichtgroep_query.replace('{uuid}', '00000000-0000-3000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('00000000-0000-0000-1000-000000000000', result)

        processor.process(uuids=['00000000-0000-1000-0000-000000000000', '00000000-0000-2000-0000-000000000000',
                                 '00000000-0000-3000-0000-000000000000'])

        with self.subTest('check asset1 after toezicht updated'):
            cursor.execute(
                select_toezichter_query.replace('{uuid}', '00000000-0000-1000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('10000000-0000-0000-0000-000000000000', result)
            cursor.execute(
                select_toezichtgroep_query.replace('{uuid}', '00000000-0000-1000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('00000000-0000-0000-1000-000000000000', result)
        with self.subTest('check asset2 after toezicht updated'):
            cursor.execute(
                select_toezichter_query.replace('{uuid}', '00000000-0000-2000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual(None, result)
            cursor.execute(
                select_toezichtgroep_query.replace('{uuid}', '00000000-0000-2000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual(None, result)
        with self.subTest('check asset3 after toezicht updated'):
            cursor.execute(
                select_toezichter_query.replace('{uuid}', '00000000-0000-3000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('20000000-0000-0000-0000-000000000000', result)
            cursor.execute(
                select_toezichtgroep_query.replace('{uuid}', '00000000-0000-3000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('00000000-0000-0000-2000-000000000000', result)

    def test_missing_toezichtgroep(self):
        self.setup()

        cursor = self.connector.connection.cursor()
        self.set_up_one_asset(cursor)
        self.set_up_identiteiten(cursor)

        self.connector.commit_transaction()

        processor = ToezichtGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer)
        processor.em_infra_importer.import_assets_from_webservice_by_uuids = self.return_assets

        with self.assertRaises(ToezichtgroepMissingError):
            processor.process(['00000000-0000-1000-0000-000000000000'])
        self.connector.connection.close()

    def test_missing_identiteit(self):
        self.setup()

        cursor = self.connector.connection.cursor()
        self.set_up_one_asset(cursor)

        self.connector.commit_transaction()

        processor = ToezichtGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer)
        processor.em_infra_importer.import_assets_from_webservice_by_uuids = self.return_assets

        with self.assertRaises(IdentiteitMissingError):
            processor.process(['00000000-0000-1000-0000-000000000000'])
        self.connector.connection.close()

    def set_up_identiteiten(self, cursor):
        cursor.execute("""
        INSERT INTO identiteiten (uuid, naam, gebruikersnaam, actief, systeem) 
        VALUES ('10000000-0000-0000-0000-000000000000', 'persoon1', 'persoon1', TRUE, FALSE),
            ('20000000-0000-0000-0000-000000000000', 'persoon2', 'persoon2', TRUE, FALSE)""")

    def set_up_toezichtgroepen(self, cursor):
        cursor.execute("""
        INSERT INTO toezichtgroepen (uuid, naam, referentie, actief) 
        VALUES ('00000000-0000-0000-1000-000000000000', 'groep1', 'groep1', TRUE),
            ('00000000-0000-0000-2000-000000000000', 'groep2', 'groep2', TRUE)""")

    def set_up_one_asset(self, cursor):
        cursor.execute("""
        INSERT INTO assettypes (uuid, naam, label, uri, actief) 
        VALUES ('00000000-1000-0000-0000-000000000000', 'type1', 'type1', 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#type1', TRUE)""")

        cursor.execute("""
        INSERT INTO assets (uuid, naam, assettype, toezichter, toezichtgroep, actief) 
        VALUES ('00000000-0000-1000-0000-000000000000', 'asset1',
            '00000000-1000-0000-0000-000000000000', NULL, NULL, TRUE)""")

    def set_up_assets(self, cursor):
        cursor.execute("""
        INSERT INTO assettypes (uuid, naam, label, uri, actief) 
        VALUES ('00000000-1000-0000-0000-000000000000', 'type1', 'type1', 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#type1', TRUE)""")

        cursor.execute("""
        INSERT INTO assets (uuid, naam, assettype, toezichter, toezichtgroep, actief) 
        VALUES ('00000000-0000-1000-0000-000000000000','asset1','00000000-1000-0000-0000-000000000000',NULL,NULL,TRUE),
        ('00000000-0000-2000-0000-000000000000', 'asset2', '00000000-1000-0000-0000-000000000000', 
            '10000000-0000-0000-0000-000000000000', '00000000-0000-0000-1000-000000000000', TRUE),
        ('00000000-0000-3000-0000-000000000000', 'asset3', '00000000-1000-0000-0000-000000000000', 
            '10000000-0000-0000-0000-000000000000', '00000000-0000-0000-1000-000000000000', TRUE)""")

    def return_assets(self, asset_uuids):
        return [{
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-1000-0000-000000000000-bGdjOmluc3RhbGxhdGllI0thc3Q",
            "NaampadObject.naampad": "asset1",
            "tz:Schadebeheerder.schadebeheerder": {
                "tz:DtcBeheerder.naam": "District Geel",
                "tz:DtcBeheerder.referentie": "114"
            },
            "AIMDBStatus.isActief": True,
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "00000000-0000-1000-0000-000000000000-bGdjOmluc3RhbGxhdGllI0thc3Q",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "tz:Toezicht.toezichtgroep": {
                "tz:DtcToezichtGroep.referentie": "groep1",
                "tz:DtcToezichtGroep.naam": "groep1"
            },
            "tz:Toezicht.toezichter": {
                "tz:DtcToezichter.email": "persoon1@mow.vlaanderen.be",
                "tz:DtcToezichter.voornaam": "persoon1",
                "tz:DtcToezichter.naam": "persoon1",
                "tz:DtcToezichter.gebruikersnaam": "persoon1"
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
                "tz:DtcBeheerder.naam": "District Geel",
                "tz:DtcBeheerder.referentie": "114"
            },
            "AIMDBStatus.isActief": True,
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "00000000-0000-3000-0000-000000000000-bGdjOmluc3RhbGxhdGllI0thc3Q",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "tz:Toezicht.toezichtgroep": {
                "tz:DtcToezichtGroep.referentie": "groep2",
                "tz:DtcToezichtGroep.naam": "groep2"
            },
            "tz:Toezicht.toezichter": {
                "tz:DtcToezichter.email": "persoon2@mow.vlaanderen.be",
                "tz:DtcToezichter.voornaam": "persoon2",
                "tz:DtcToezichter.naam": "persoon2",
                "tz:DtcToezichter.gebruikersnaam": "persoon2"
            }
        }]
