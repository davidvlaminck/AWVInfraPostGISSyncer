from unittest import TestCase

from psycopg2 import connect

from AssetSyncer import AssetSyncer
from AssetTypeSyncer import AssetTypeSyncer
from EMInfraImporter import EMInfraImporter
from EventProcessors.ElekAansluitingGewijzigdProcessor import ElekAansluitingGewijzigdProcessor
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager


class ElekAansluitingGewijzigdProcessorTests(TestCase):
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

        self.processor = ElekAansluitingGewijzigdProcessor(cursor=self.connector.connection.cursor(),
                                                           em_infra_importer=self.eminfra_importer)
        self.assettypes_syncer = AssetTypeSyncer(postGIS_connector=self.connector,
                                                 emInfraImporter=self.eminfra_importer)
        self.assets_syncer = AssetSyncer(postgis_connector=self.connector, em_infra_importer=self.eminfra_importer)

    def test_update_elek_aansluiting(self):
        self.set_up_assets()

        cursor = self.connector.connection.cursor()
        cursor.execute(
            "INSERT INTO elek_aansluitingen (assetUuid, EAN, aansluiting) VALUES ('00114756-9f8b-4c8c-b905-93da6c0b26cd','540000000000000001','FICTIEF')")

        select_aansluiting_query = "SELECT EAN FROM elek_aansluitingen WHERE assetUuid = '{uuid}'"
        count_aansluiting_query = "SELECT count(*) FROM elek_aansluitingen"

        with self.subTest('aansluiting check of the first asset'):
            cursor.execute(select_aansluiting_query.replace('{uuid}', '00114756-9f8b-4c8c-b905-93da6c0b26cd'))
            result = cursor.fetchone()[0]
            self.assertEqual('540000000000000001', result)
        with self.subTest('number of aansluitingen before update'):
            cursor.execute(count_aansluiting_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        self.processor.em_infra_importer.get_all_elek_aansluitingen_from_webservice_by_asset_uuids = self.return_elek_aansluiting_dicts

        self.processor.process(['00114756-9f8b-4c8c-b905-93da6c0b26cd', '89daf088-2900-11ed-a261-0242ac120002'])

        with self.subTest('aansluiting check of the first asset'):
            cursor.execute(select_aansluiting_query.replace('{uuid}', '00114756-9f8b-4c8c-b905-93da6c0b26cd'))
            result = cursor.fetchone()[0]
            self.assertEqual('540000000000000002', result)
        with self.subTest('aansluiting check of the second asset'):
            cursor.execute(select_aansluiting_query.replace('{uuid}', '89daf088-2900-11ed-a261-0242ac120002'))
            result = cursor.fetchone()[0]
            self.assertEqual('540000000000000003', result)
        with self.subTest('number of aansluitingen after update'):
            cursor.execute(count_aansluiting_query)
            result = cursor.fetchone()[0]
            self.assertEqual(2, result)

    @staticmethod
    def return_elek_aansluiting_dicts(asset_uuids):
        yield from [('00114756-9f8b-4c8c-b905-93da6c0b26cd', [{
            "_type": "87dff279-4162-4031-ba30-fb7ffd9c014b",
            "type": {
                "uuid": "87dff279-4162-4031-ba30-fb7ffd9c014b",
                "createdOn": "2019-01-15T09:55:37.318+01:00",
                "modifiedOn": "2019-02-21T16:04:25.566+01:00",
                "naam": "Elektrisch aansluitpunt",
                "actief": True,
                "predefined": True,
                "standard": False,
                "definitie": "Heeft een elektrisch aansluitpunt."
            },
            "elektriciteitsAansluitingRef": {
                "uuid": "640cd848-e3d4-4ee6-ab48-d016d437f2cc",
                "amid": "2",
                "ean": "540000000000000002",
                "aansluitnummer": "FICTIEF_2"
            },
            "links": []
        }]), ('89daf088-2900-11ed-a261-0242ac120002', [{
            "_type": "87dff279-4162-4031-ba30-fb7ffd9c014b",
            "type": {
                "uuid": "87dff279-4162-4031-ba30-fb7ffd9c014b",
                "createdOn": "2019-01-15T09:55:37.318+01:00",
                "modifiedOn": "2019-02-21T16:04:25.566+01:00",
                "naam": "Elektrisch aansluitpunt",
                "actief": True,
                "predefined": True,
                "standard": False,
                "definitie": "Heeft een elektrisch aansluitpunt."
            },
            "elektriciteitsAansluitingRef": {
                "uuid": "640cd848-e3d4-4ee6-ab48-d016d437f2cc",
                "amid": "3",
                "ean": "540000000000000003",
                "aansluitnummer": "FICTIEF_3"
            },
            "links": []
        }])]

    def set_up_assettypes(self):
        assettypes = [{
            "_type": "installatietype",
            "uuid": "80fdf1b4-e311-4270-92ba-6367d2a42d47",
            "createdOn": "2019-01-16T14:13:09.335+01:00",
            "modifiedOn": "2022-02-28T14:39:32.136+01:00",
            "uri": "https://lgc.data.wegenenverkeer.be/ns/installatie#LS",
            "korteUri": "lgc:installatie#LS",
            "afkorting": "LS",
            "naam": "Laagspanningsaansluiting",
            "actief": True,
            "definitie": "Laagspanningsaansluiting. Deze zit meestal in een Kast of Cabine (behuizing) en voedt meestal een LSDeel."
        }]
        self.assettypes_syncer.update_assettypes(assettypes_dicts=assettypes)

    def set_up_assets(self):
        self.setup()

        self.set_up_assettypes()

        assets = [{
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#LS",
            "@id": "https://data.awvvlaanderen.be/id/asset/00114756-9f8b-4c8c-b905-93da6c0b26cd-bGdjOmluc3RhbGxhdGllI0xT",
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/verwijderd",
            "AIMObject.notitie": "",
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "00114756-9f8b-4c8c-b905-93da6c0b26cd-bGdjOmluc3RhbGxhdGllI0xT",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "NaampadObject.naampad": "N303X8.1/WZ1221/LS-C",
            "AIMNaamObject.naam": "LS-C",
            "AIMDBStatus.isActief": False,
            "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#LS"
        }, {
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#LS",
            "@id": "https://data.awvvlaanderen.be/id/asset/89daf088-2900-11ed-a261-0242ac120002-bGdjOmluc3RhbGxhdGllI0xT",
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/verwijderd",
            "AIMObject.notitie": "",
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "89daf088-2900-11ed-a261-0242ac120002-bGdjOmluc3RhbGxhdGllI0xT",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "NaampadObject.naampad": "DUMMY/RANDOM_GENERATED_LS",
            "AIMNaamObject.naam": "RANDOM_GENERATED_LS",
            "AIMDBStatus.isActief": True,
            "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#LS"
        }]

        self.assets_syncer.update_assets(assets_dicts=assets, cursor=self.connector.connection.cursor())
