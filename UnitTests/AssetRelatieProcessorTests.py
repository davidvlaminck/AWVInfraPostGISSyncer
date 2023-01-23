from unittest import TestCase
from unittest.mock import MagicMock

from psycopg2 import connect

from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetRelatiesGewijzigdProcessor import AssetRelatiesGewijzigdProcessor
from Exceptions.AssetMissingError import AssetMissingError
from PostGISConnector import PostGISConnector
from SettingsManager import SettingsManager


class AssetRelatieProcessorTests(TestCase):
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

    def test_update_assetRelaties(self):
        self.setup()

        cursor = self.connector.connection.cursor()
        self.set_up_assets(cursor)
        self.set_up_relatietype(cursor)

        create_assetrelatie_query = """
        INSERT INTO assetRelaties (uuid, bronUuid, doelUuid, relatietype, actief) 
        VALUES ('00000000-0000-5000-0000-000000000000', '20000000-0000-0000-0000-000000000000', 
        '10000000-0000-0000-0000-000000000000', '00000000-0000-0000-1000-000000000000', TRUE)"""
        select_bron_assetrelatie_query = "SELECT bronUuid FROM assetRelaties WHERE uuid = '{uuid}'"
        select_doel_assetrelatie_query = "SELECT doelUuid FROM assetRelaties WHERE uuid = '{uuid}'"
        select_attributen_assetrelatie_query = "SELECT attributen FROM assetRelaties WHERE uuid = '{uuid}'"
        count_assetrelatie_query = "SELECT count(*) FROM assetRelaties"
        cursor = self.connector.connection.cursor()
        cursor.execute(create_assetrelatie_query)

        with self.subTest('check the assetrelatie created'):
            cursor.execute(select_bron_assetrelatie_query.replace('{uuid}', '00000000-0000-5000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('20000000-0000-0000-0000-000000000000', result)
            cursor.execute(select_doel_assetrelatie_query.replace('{uuid}', '00000000-0000-5000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('10000000-0000-0000-0000-000000000000', result)
        with self.subTest('number of assetRelaties before update'):
            cursor.execute(count_assetrelatie_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        processor = AssetRelatiesGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer,
                                                    connector=self.connector)
        processor.em_infra_importer.import_assetrelaties_from_webservice_by_assetuuids = self.return_assetrelaties

        processor.process(['10000000-0000-0000-0000-000000000000', '20000000-0000-0000-0000-000000000000',
                           '30000000-0000-0000-0000-000000000000', '40000000-0000-0000-0000-000000000000'])

        with self.subTest('check after the assetrelatie updated'):
            cursor.execute(select_bron_assetrelatie_query.replace('{uuid}', '00000000-0000-1000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('10000000-0000-0000-0000-000000000000', result)
            cursor.execute(select_doel_assetrelatie_query.replace('{uuid}', '00000000-0000-1000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('20000000-0000-0000-0000-000000000000', result)
        with self.subTest('check after the assetrelatie created'):
            cursor.execute(select_bron_assetrelatie_query.replace('{uuid}', '00000000-0000-4000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('20000000-0000-0000-0000-000000000000', result)
        with self.subTest('check attributen after assetrelaties created'):
            cursor.execute(select_attributen_assetrelatie_query.replace('{uuid}', '00000000-0000-4000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual("{'Voedt.aansluitspanning': 0}", result)
        with self.subTest('number of assetRelaties after update'):
            cursor.execute(count_assetrelatie_query)
            result = cursor.fetchone()[0]
            self.assertEqual(4, result)

    def test_missing_asset(self):
        self.setup()

        cursor = self.connector.connection.cursor()
        self.set_up_missing_assets(cursor)
        self.set_up_relatietype(cursor)

        self.connector.commit_transaction()

        processor = AssetRelatiesGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer,
                                                    connector=self.connector)
        processor.em_infra_importer.import_assetrelaties_from_webservice_by_assetuuids = self.return_assetrelaties

        with self.assertRaises(AssetMissingError) as exc:
            processor.process(['10000000-0000-0000-0000-000000000000', '20000000-0000-0000-0000-000000000000',
                               '30000000-0000-0000-0000-000000000000', '40000000-0000-0000-0000-000000000000'])
        self.assertListEqual(['10000000-0000-0000-0000-000000000000'], exc.exception.args[0])
        self.connector.connection.close()

    def set_up_relatietype(self, cursor):
        cursor.execute("""
        INSERT INTO public.relatietypes (uuid, naam, uri, actief, gericht) 
        VALUES ('00000000-0000-0000-1000-000000000000', 'HoortBij', 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij', TRUE, TRUE);""")

    def set_up_missing_assets(self, cursor):
        self.set_up_assets(cursor)
        cursor.execute("""DELETE FROM public.assets WHERE uuid = '10000000-0000-0000-0000-000000000000';""")

    def set_up_assets(self, cursor):
        cursor.execute("""
        INSERT INTO public.assettypes (uuid, naam, label, uri, actief) 
        VALUES ('00000000-1000-0000-0000-000000000000', 'type1', 'type1', 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#type1', TRUE);""")

        cursor.execute("""
        INSERT INTO public.assets (uuid, naam, assettype, actief) 
        VALUES ('10000000-0000-0000-0000-000000000000', 'asset1', '00000000-1000-0000-0000-000000000000', TRUE),
        ('20000000-0000-0000-0000-000000000000', 'asset2', '00000000-1000-0000-0000-000000000000', TRUE),
        ('30000000-0000-0000-0000-000000000000', 'asset3', '00000000-1000-0000-0000-000000000000', TRUE),
        ('40000000-0000-0000-0000-000000000000', 'asset3', '00000000-1000-0000-0000-000000000000', TRUE);""")

    def return_assetrelaties(self, asset_uuids):
        return [{
            "@type": "",
            "@id": "",
            "RelatieObject.bron": {
                "@type": "",
                "@id": ""
            },
            "RelatieObject.doel": {
                "@type": "",
                "@id": ""
            },
            "RelatieObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij",
            "RelatieObject.doelAssetId": {
                "DtcIdentificator.identificator": "20000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "RelatieObject.assetId": {
                "DtcIdentificator.identificator": "00000000-0000-1000-0000-000000000000-b25kZXJkZWVsI0hvb3J0Qmlq",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "AIMDBStatus.isActief": True,
            "RelatieObject.bronAssetId": {
                "DtcIdentificator.identificator": "10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            }
        }, {
            "@type": "",
            "@id": "",
            "RelatieObject.bron": {
                "@type": "",
                "@id": ""
            },
            "RelatieObject.doel": {
                "@type": "",
                "@id": ""
            },
            "RelatieObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij",
            "RelatieObject.doelAssetId": {
                "DtcIdentificator.identificator": "30000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "RelatieObject.assetId": {
                "DtcIdentificator.identificator": "00000000-0000-2000-0000-000000000000-b25kZXJkZWVsI0hvb3J0Qmlq",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "AIMDBStatus.isActief": True,
            "RelatieObject.bronAssetId": {
                "DtcIdentificator.identificator": "10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            }
        }, {
            "@type": "",
            "@id": "",
            "RelatieObject.bron": {
                "@type": "",
                "@id": ""
            },
            "RelatieObject.doel": {
                "@type": "",
                "@id": ""
            },
            "RelatieObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij",
            "RelatieObject.doelAssetId": {
                "DtcIdentificator.identificator": "40000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "RelatieObject.assetId": {
                "DtcIdentificator.identificator": "00000000-0000-3000-0000-000000000000-b25kZXJkZWVsI0hvb3J0Qmlq",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "AIMDBStatus.isActief": True,
            "RelatieObject.bronAssetId": {
                "DtcIdentificator.identificator": "10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            }
        }, {
            "@type": "",
            "@id": "",
            "RelatieObject.bron": {
                "@type": "",
                "@id": ""
            },
            "RelatieObject.doel": {
                "@type": "",
                "@id": ""
            },
            "RelatieObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij",
            "RelatieObject.doelAssetId": {
                "DtcIdentificator.identificator": "40000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "RelatieObject.assetId": {
                "DtcIdentificator.identificator": "00000000-0000-4000-0000-000000000000-b25kZXJkZWVsI0hvb3J0Qmlq",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "AIMDBStatus.isActief": True,
            "RelatieObject.bronAssetId": {
                "DtcIdentificator.identificator": "20000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "Voedt.aansluitspanning": 0
        }]

