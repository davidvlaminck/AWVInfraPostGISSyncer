from unittest import TestCase

from psycopg2 import connect

from AssetSyncer import AssetSyncer
from AssetTypeSyncer import AssetTypeSyncer
from EMInfraImporter import EMInfraImporter
from EventProcessors.NaamGewijzigdProcessor import NaamGewijzigdProcessor
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager


class NaamGewijzigdProcessorTests(TestCase):
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

        self.processor = NaamGewijzigdProcessor(cursor=self.connector.connection.cursor(),
                                                em_infra_importer=self.eminfra_importer)
        self.assettypes_syncer = AssetTypeSyncer(postGIS_connector=self.connector,
                                                 emInfraImporter=self.eminfra_importer)
        self.assets_syncer = AssetSyncer(postgis_connector=self.connector, em_infra_importer=self.eminfra_importer)

    def test_update_naam(self):
        self.set_up_assets()

        select_asset_query = "SELECT naam, naampad FROM assets WHERE uuid = '{uuid}'"
        cursor = self.connector.connection.cursor()

        with self.subTest('naam check of the first asset'):
            cursor.execute(select_asset_query.replace('{uuid}', '00000453-56ce-4f8b-af44-960df526cb30'))
            result = cursor.fetchmany()[0]
            self.assertEqual('slechte_naam_voor_kast', result[0])
            self.assertEqual('057A5/slechte_naam_voor_kast', result[1])
        with self.subTest('naam check of the second asset'):
            cursor.execute(select_asset_query.replace('{uuid}', '00088892-53a8-4dfc-a2c9-875cab2d7e11'))
            result = cursor.fetchmany()[0]
            self.assertEqual('N50N618-AS1.Fa1.8', result[0])
            self.assertEqual(None, result[1])

        self.processor.em_infra_importer.import_assets_from_webservice_by_uuids = self.return_new_asset_dicts

        self.processor.process(['00000453-56ce-4f8b-af44-960df526cb30', '00088892-53a8-4dfc-a2c9-875cab2d7e11'])

        with self.subTest('naam check of the first asset'):
            cursor.execute(select_asset_query.replace('{uuid}', '00000453-56ce-4f8b-af44-960df526cb30'))
            result = cursor.fetchmany()[0]
            self.assertEqual('KAST', result[0])
            self.assertEqual('057A5/KAST', result[1])
        with self.subTest('naam check of the second asset'):
            cursor.execute(select_asset_query.replace('{uuid}', '00088892-53a8-4dfc-a2c9-875cab2d7e11'))
            result = cursor.fetchmany()[0]
            self.assertEqual('N50N618-AS1.Fa1.8', result[0])
            self.assertEqual(None, result[1])

    @staticmethod
    def return_new_asset_dicts(asset_uuids):
        return [{
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000453-56ce-4f8b-af44-960df526cb30-bGdjOmluc3RhbGxhdGllI0thc3Q",
            "NaampadObject.naampad": "057A5/KAST",
            "AIMObject.notitie": "notitie na update",
            "tz:Schadebeheerder.schadebeheerder": {
                "tz:DtcBeheerder.naam": "District Geel",
                "tz:DtcBeheerder.referentie": "114"
            },
            "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
            "AIMDBStatus.isActief": True,
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "00000453-56ce-4f8b-af44-960df526cb30-bGdjOmluc3RhbGxhdGllI0thc3Q",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "tz:Toezicht.toezichtgroep": {
                "tz:DtcToezichtGroep.referentie": "AWV_EW_AN",
                "tz:DtcToezichtGroep.naam": "AWV_EW_AN"
            },
            "AIMNaamObject.naam": "KAST",
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/uit-gebruik",
            "loc:Locatie.geometrie": "",
            "loc:Locatie.omschrijving": "N156 JANSSEN PHARMACEUTICALAAN - AMOCOLAAN - OOSTERLOSEWEG",
            "tz:Toezicht.toezichter": {
                "tz:DtcToezichter.email": "niels.vanasch@mow.vlaanderen.be",
                "tz:DtcToezichter.voornaam": "Niels",
                "tz:DtcToezichter.naam": "Van Asch",
                "tz:DtcToezichter.gebruikersnaam": "vanascni"
            }
        },  {
                "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
                "@id": "https://data.awvvlaanderen.be/id/asset/00088892-53a8-4dfc-a2c9-875cab2d7e11-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/verwijderd",
                "Netwerkpoort.type": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkpoortType/UNI",
                "AIMDBStatus.isActief": False,
                "AIMNaamObject.naam": "N50N618-AS1.Fa1.8",
                "AIMObject.assetId": {
                    "DtcIdentificator.identificator": "00088892-53a8-4dfc-a2c9-875cab2d7e11-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                    "DtcIdentificator.toegekendDoor": "AWV"
                },
                "Netwerkpoort.beschrijvingFabrikant": "NULL",
                "Netwerkpoort.config": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkpoortConfig/FE",
                "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
                "Netwerkpoort.golflengte": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkpoortGolflengte/NULL",
                "AIMObject.notitie": "",
                "Netwerkpoort.nNILANCapaciteit": 10,
                "Netwerkpoort.technologie": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkTechnologie/NULL",
                "Netwerkpoort.merk": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkMerk/Cisco",
                "Netwerkpoort.serienummer": "NULL"
            }]

    def set_up_assettypes(self):
        assettypes = [{
            "_type": "onderdeeltype",
            "uuid": "6b3dba37-7b73-4346-a264-f4fe5b796c02",
            "createdOn": "2019-12-16T20:33:52.303+01:00",
            "modifiedOn": "2022-06-13T14:27:13.874+02:00",
            "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
            "korteUri": "onderdeel#Netwerkpoort",
            "afkorting": "Netwerkpoort",
            "naam": "Netwerkpoort",
            "actief": True,
            "definitie": "De ingang van het toestel samen met component die erop zit,bv. SFP of XFP."
        }, {
            "_type": "installatietype",
            "uuid": "10377658-776f-4c21-a294-6c740b9f655e",
            "createdOn": "2019-01-16T14:13:14.844+01:00",
            "modifiedOn": "2022-05-25T19:39:15.608+02:00",
            "uri": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
            "korteUri": "lgc:installatie#Kast",
            "afkorting": "Kast",
            "naam": "Kast",
            "actief": True,
            "definitie": "Installatiekast of Voetpadkast - fysieke behuizing"
        }]
        self.assettypes_syncer.update_assettypes(assettypes_dicts=assettypes)

    def set_up_assets(self):
        self.setup()

        self.set_up_assettypes()

        assets = [{
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
            "@id": "https://data.awvvlaanderen.be/id/asset/00000453-56ce-4f8b-af44-960df526cb30-bGdjOmluc3RhbGxhdGllI0thc3Q",
            "NaampadObject.naampad": "057A5/slechte_naam_voor_kast",
            "AIMObject.notitie": "notitie voor update",
            "tz:Schadebeheerder.schadebeheerder": {
                "tz:DtcBeheerder.naam": "District Geel",
                "tz:DtcBeheerder.referentie": "114"
            },
            "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
            "AIMDBStatus.isActief": False,
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "00000453-56ce-4f8b-af44-960df526cb30-bGdjOmluc3RhbGxhdGllI0thc3Q",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "tz:Toezicht.toezichtgroep": {
                "tz:DtcToezichtGroep.referentie": "AWV_EW_AN",
                "tz:DtcToezichtGroep.naam": "AWV_EW_AN"
            },
            "AIMNaamObject.naam": "slechte_naam_voor_kast",
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-opbouw",
            "loc:Locatie.geometrie": "",
            "loc:Locatie.omschrijving": "N156 JANSSEN PHARMACEUTICALAAN - AMOCOLAAN - OOSTERLOSEWEG",
            "tz:Toezicht.toezichter": {
                "tz:DtcToezichter.email": "niels.vanasch@mow.vlaanderen.be",
                "tz:DtcToezichter.voornaam": "Niels",
                "tz:DtcToezichter.naam": "Van Asch",
                "tz:DtcToezichter.gebruikersnaam": "vanascni"
            }
        },  {
                "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
                "@id": "https://data.awvvlaanderen.be/id/asset/00088892-53a8-4dfc-a2c9-875cab2d7e11-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
                "Netwerkpoort.type": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkpoortType/UNI",
                "AIMDBStatus.isActief": True,
                "AIMNaamObject.naam": "N50N618-AS1.Fa1.8",
                "AIMObject.assetId": {
                    "DtcIdentificator.identificator": "00088892-53a8-4dfc-a2c9-875cab2d7e11-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                    "DtcIdentificator.toegekendDoor": "AWV"
                },
                "Netwerkpoort.beschrijvingFabrikant": "NULL",
                "Netwerkpoort.config": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkpoortConfig/FE",
                "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
                "Netwerkpoort.golflengte": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkpoortGolflengte/NULL",
                "AIMObject.notitie": "notitie voor aanpassing",
                "Netwerkpoort.nNILANCapaciteit": 10,
                "Netwerkpoort.technologie": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkTechnologie/NULL",
                "Netwerkpoort.merk": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkMerk/Cisco",
                "Netwerkpoort.serienummer": "NULL"
            }]

        self.assets_syncer.update_assets(assets_dicts=assets, cursor=self.connector.connection.cursor())



