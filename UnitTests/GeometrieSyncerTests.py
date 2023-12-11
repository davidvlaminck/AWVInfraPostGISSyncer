from unittest import TestCase

from psycopg2 import connect

from AssetSyncer import AssetSyncer
from AssetTypeSyncer import AssetTypeSyncer
from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.GeometrieOrLocatieGewijzigdProcessor import GeometrieOrLocatieGewijzigdProcessor
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager


class GeometrieSyncerTests(TestCase):
    def setup(self):

        self.connector = PostGISConnector(host=unittest_db_settings['host'], port=unittest_db_settings['port'],
                                          user=unittest_db_settings['user'], password=unittest_db_settings['password'],
                                          database="unittests")
        self.connector.set_up_tables('../setup_tables_querys.sql')

        requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
        request_handler = RequestHandler(requester)
        self.eminfra_importer = EMInfraImporter(request_handler)

        self.assettypes_syncer = AssetTypeSyncer(postGIS_connector=self.connector,
                                                 emInfraImporter=self.eminfra_importer)
        self.assets_syncer = AssetSyncer(postgis_connector=self.connector, em_infra_importer=self.eminfra_importer)
        self.processor = GeometrieOrLocatieGewijzigdProcessor(cursor=self.connector.connection.cursor(),
                                                              eminfra_importer=self.eminfra_importer)

    def test_update_geometries(self):
        self.setup()

        self.set_up_assettypes()

        cursor = self.connector.connection.cursor()
        self.set_up_assets(cursor)
        insert_geometrie_wkt_query = "INSERT INTO geometrie (assetUuid, niveau, wkt_string) VALUES ('5dbca334-9ce8-4ebe-80c3-01c01dd1844f', -1, 'POINT Z (0 0 0)')"
        cursor.execute(insert_geometrie_wkt_query)
        insert_locatie_omschrijving_query = "INSERT INTO locatie (assetUuid, omschrijving, geometrie) VALUES ('5dbca334-9ce8-4ebe-80c3-01c01dd1844f', 'test omschrijving', 'POINT Z (0 0 0)')"
        cursor.execute(insert_locatie_omschrijving_query)
        # TODO should be removed when creating an asset also supports creating locations

        select_geometrie_wkt_query = "SELECT wkt_string FROM geometrie WHERE assetUuid = '{uuid}' and niveau = {niveau}"
        count_geometrie_query = "SELECT count(*) FROM geometrie"
        select_locatie_omschrijving_query = "SELECT omschrijving FROM locatie WHERE assetUuid = '{uuid}'"
        count_locatie_query = "SELECT count(*) FROM locatie"

        with self.subTest('geometrie wkt check for the first asset'):
            cursor.execute(select_geometrie_wkt_query
                           .replace('{uuid}', '5dbca334-9ce8-4ebe-80c3-01c01dd1844f')
                           .replace('{niveau}', '-1'))
            result = cursor.fetchone()[0]
            self.assertEqual('POINT Z (0 0 0)', result)
        with self.subTest('number of geometrie records before update'):
            cursor.execute(count_geometrie_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)
        with self.subTest('locatie omschrijving check for the first asset'):
            cursor.execute(select_locatie_omschrijving_query.replace('{uuid}', '5dbca334-9ce8-4ebe-80c3-01c01dd1844f'))
            result = cursor.fetchone()[0]
            self.assertEqual('test omschrijving', result)
        with self.subTest('number of locatie records before update'):
            cursor.execute(count_locatie_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        self.processor.eminfra_importer.import_assets_from_webservice_by_uuids = self.return_new_asset_dicts

        self.processor.process(['00000453-56ce-4f8b-af44-960df526cb30', '00088892-53a8-4dfc-a2c9-875cab2d7e11',
                                '5dbca334-9ce8-4ebe-80c3-01c01dd1844f'])

        with self.subTest('geometrie wkt check for the first asset'):
            cursor.execute(select_geometrie_wkt_query
                           .replace('{uuid}', '5dbca334-9ce8-4ebe-80c3-01c01dd1844f')
                           .replace('{niveau}', '-1'))
            result = cursor.fetchone()[0]
            self.assertEqual('POINT Z (0 0 0)', result)
            cursor.execute(select_geometrie_wkt_query
                           .replace('{uuid}', '5dbca334-9ce8-4ebe-80c3-01c01dd1844f')
                           .replace('{niveau}', '0'))
            result = cursor.fetchone()[0]
            self.assertEqual(
                'POLYGON Z ((164980.5 172859.4 0,165004.77 172855.89 0,165003 172849.2 0,164980.5 172859.4 0))', result)
        with self.subTest('number of geometrie records after update'):
            cursor.execute(count_geometrie_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)
        with self.subTest('locatie omschrijving check for the first asset'):
            cursor.execute(select_locatie_omschrijving_query.replace('{uuid}', '5dbca334-9ce8-4ebe-80c3-01c01dd1844f'))
            result = cursor.fetchone()[0]
            self.assertEqual('ingevulde locatie omschrijving', result)
        with self.subTest('number of locatie records after update'):
            cursor.execute(count_locatie_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)

    @staticmethod
    def return_new_asset_dicts(asset_uuids):
        return [
            {
                "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
                "@id": "https://data.awvvlaanderen.be/id/asset/000ad2af-e393-4c45-b54f-b0d94524b1e1-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "AIMObject.assetId": {
                    "DtcIdentificator.identificator": "000ad2af-e393-4c45-b54f-b0d94524b1e1-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                    "DtcIdentificator.toegekendDoor": "AWV"
                },
                "loc:Locatie.omschrijving": "",
                "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
                "Netwerkpoort.nNILANCapaciteit": 1000,
                "Netwerkpoort.config": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkpoortConfig/GE",
                "Netwerkpoort.beschrijvingFabrikant": "NULL",
                "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
                "Netwerkpoort.golflengte": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkpoortGolflengte/NULL",
                "AIMObject.notitie": "",
                "Netwerkpoort.technologie": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkTechnologie/NULL",
                "geo:Geometrie.log": [
                    {
                        "geo:DtcLog.gaVersie": "GA_2.3.0",
                        "geo:DtcLog.geometrie": {
                            "geo:DtuGeometrie.punt": "POINT Z(153759.7 211533.4 0)"
                        },
                        "geo:DtcLog.bron": "https://geo.data.wegenenverkeer.be/id/concept/KlLogBron/overerving",
                        "geo:DtcLog.niveau": "https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/-1",
                        "geo:DtcLog.overerving": [
                            {
                                "geo:DtcOvererving.erflaatId": {
                                    "DtcIdentificator.toegekendDoor": "AWV",
                                    "DtcIdentificator.identificator": "0e5438db-0be8-405a-b350-f262dc781b1c-b25kZXJkZWVsI05ldHdlcmtlbGVtZW50"
                                },
                                "geo:DtcOvererving.relatieId": {
                                    "DtcIdentificator.identificator": "e612be08-74b6-46ed-9046-8f82ae0d7b48-b25kZXJkZWVsI0JldmVzdGlnaW5n",
                                    "DtcIdentificator.toegekendDoor": "AWV"
                                },
                                "geo:DtcOvererving.erfgenaamId": {
                                    "DtcIdentificator.identificator": "000ad2af-e393-4c45-b54f-b0d94524b1e1-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                                    "DtcIdentificator.toegekendDoor": "AWV"
                                }
                            }
                        ],
                        "geo:DtcLog.nauwkeurigheid": ""
                    }
                ],
                "loc:Locatie.puntlocatie": {
                    "loc:DtcPuntlocatie.precisie": "",
                    "loc:3Dpunt.puntgeometrie": {
                        "loc:DtcCoord.lambert72": {
                            "loc:DtcCoordLambert72.xcoordinaat": 153759.7,
                            "loc:DtcCoordLambert72.ycoordinaat": 211533.4,
                            "loc:DtcCoordLambert72.zcoordinaat": 0
                        }
                    },
                    "loc:DtcPuntlocatie.adres": {
                        "loc:DtcAdres.bus": "",
                        "loc:DtcAdres.gemeente": "Antwerpen",
                        "loc:DtcAdres.provincie": "Antwerpen",
                        "loc:DtcAdres.postcode": "2018",
                        "loc:DtcAdres.nummer": "20",
                        "loc:DtcAdres.straat": "Kievitplein"
                    },
                    "loc:DtcPuntlocatie.weglocatie": "",
                    "loc:DtcPuntlocatie.bron": ""
                },
                "loc:Locatie.geometrie": "POINT Z(153759.7 211533.4 0)",
                "AIMNaamObject.naam": "VCN-ANT-CS01.Gi4.0.15",
                "AIMDBStatus.isActief": True,
                "Netwerkpoort.type": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkpoortType/ncni",
                "Netwerkpoort.merk": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlNetwerkMerk/Cisco",
                "Netwerkpoort.serienummer": "NULL",
                "wl:Weglocatie.wegaanduiding": [
                    {
                        "wl:DtcWegaanduiding.tot": {
                            "wl:DtcRelatieveLocatie.weg": {
                                "wl:DtcWeg.nummer": "N7530002"
                            },
                            "wl:DtcRelatieveLocatie.referentiepunt": {
                                "wl:DtcReferentiepunt.weg": {
                                    "wl:DtcWeg.nummer": "N7530001"
                                },
                                "wl:DtcReferentiepunt.opschrift": "1.9"
                            },
                            "wl:DtcRelatieveLocatie.afstand": 647
                        },
                        "wl:DtcWegaanduiding.van": {
                            "wl:DtcRelatieveLocatie.weg": {
                                "wl:DtcWeg.nummer": "N7530002"
                            },
                            "wl:DtcRelatieveLocatie.referentiepunt": {
                                "wl:DtcReferentiepunt.weg": {
                                    "wl:DtcWeg.nummer": "N7530001"
                                },
                                "wl:DtcReferentiepunt.opschrift": "1.9"
                            },
                            "wl:DtcRelatieveLocatie.afstand": 647
                        },
                        "wl:DtcWegaanduiding.weg": {
                            "wl:DtcWeg.nummer": "N7530002"
                        }
                    }
                ],
                "wl:Weglocatie.geometrie": "POINT Z(153759.7 211533.4 0)",
                "wl:Weglocatie.wegsegment": [
                    {
                        "wl:DtcWegsegment.oidn": 347253
                    }
                ],
                "wl:Weglocatie.bron": "https://wl.data.wegenenverkeer.be/id/concept/KlWeglocatieBron/automatisch",
                "wl:Weglocatie.score": "12.782011089369375",
            },
            {
                "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
                "@id": "https://data.awvvlaanderen.be/id/asset/00000453-56ce-4f8b-af44-960df526cb30-bGdjOmluc3RhbGxhdGllI0thc3Q",
                "NaampadObject.naampad": "057A5/KAST",
                "AIMObject.notitie": "",
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
                "loc:Locatie.puntlocatie": {
                    "loc:DtcPuntlocatie.weglocatie": {
                        "loc:DtcWeglocatie.referentiepaalAfstand": 45,
                        "loc:DtcWeglocatie.ident2": "N156",
                        "loc:DtcWeglocatie.ident8": "N1560001",
                        "loc:DtcWeglocatie.gemeente": "Geel",
                        "loc:DtcWeglocatie.straatnaam": "Amocolaan",
                        "loc:DtcWeglocatie.referentiepaalOpschrift": 10.8
                    },
                    "loc:3Dpunt.puntgeometrie": {
                        "loc:DtcCoord.lambert72": {
                            "loc:DtcCoordLambert72.xcoordinaat": 192721.4,
                            "loc:DtcCoordLambert72.ycoordinaat": 201119.2,
                            "loc:DtcCoordLambert72.zcoordinaat": 0
                        }
                    },
                    "loc:DtcPuntlocatie.adres": {
                        "loc:DtcAdres.postcode": "2440",
                        "loc:DtcAdres.bus": "",
                        "loc:DtcAdres.straat": "Oosterloseweg",
                        "loc:DtcAdres.gemeente": "Geel",
                        "loc:DtcAdres.provincie": "Antwerpen",
                        "loc:DtcAdres.nummer": "36"
                    },
                    "loc:DtcPuntlocatie.precisie": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatiePrecisie/meter",
                    "loc:DtcPuntlocatie.bron": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatieBron/manueel"
                },
                "AIMNaamObject.naam": "KAST",
                "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
                "loc:Locatie.geometrie": "",
                "loc:Locatie.omschrijving": "N156 JANSSEN PHARMACEUTICALAAN - AMOCOLAAN - OOSTERLOSEWEG",
                "tz:Toezicht.toezichter": {
                    "tz:DtcToezichter.email": "niels.vanasch@mow.vlaanderen.be",
                    "tz:DtcToezichter.voornaam": "Niels",
                    "tz:DtcToezichter.naam": "Van Asch",
                    "tz:DtcToezichter.gebruikersnaam": "vanascni"
                }
            },
            {
                "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#AanvullendeGeometrie",
                "@id": "https://data.awvvlaanderen.be/id/asset/5dbca334-9ce8-4ebe-80c3-01c01dd1844f-b25kZXJkZWVsI0FhbnZ1bGxlbmRlR2VvbWV0cmll",
                "loc:Locatie.puntlocatie": {
                    "loc:DtcPuntlocatie.precisie": "",
                    "loc:3Dpunt.puntgeometrie": {
                        "loc:DtcCoord.lambert72": {
                            "loc:DtcCoordLambert72.xcoordinaat": 164980.5,
                            "loc:DtcCoordLambert72.ycoordinaat": 172859.4,
                            "loc:DtcCoordLambert72.zcoordinaat": 0
                        }
                    },
                    "loc:DtcPuntlocatie.weglocatie": {
                        "loc:DtcWeglocatie.straatnaam": "",
                        "loc:DtcWeglocatie.referentiepaalOpschrift": 14,
                        "loc:DtcWeglocatie.ident8": "A0030002",
                        "loc:DtcWeglocatie.referentiepaalAfstand": -3,
                        "loc:DtcWeglocatie.ident2": "A3",
                        "loc:DtcWeglocatie.gemeente": "Kortenberg"
                    },
                    "loc:DtcPuntlocatie.bron": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatieBron/manueel",
                    "loc:DtcPuntlocatie.adres": {
                        "loc:DtcAdres.bus": "",
                        "loc:DtcAdres.straat": "Hollestraat",
                        "loc:DtcAdres.provincie": "Vlaams-Brabant",
                        "loc:DtcAdres.nummer": "78",
                        "loc:DtcAdres.postcode": "3078",
                        "loc:DtcAdres.gemeente": "Kortenberg"
                    }
                },
                "loc:Locatie.geometrie": "POLYGON Z ((164980.5 172859.4 0,165004.77 172855.89 0,165003 172849.2 0,164980.5 172859.4 0))",
                "loc:Locatie.omschrijving": "ingevulde locatie omschrijving",
                "AbstracteAanvullendeGeometrie.assetId": {
                    "DtcIdentificator.toegekendDoor": "AWV",
                    "DtcIdentificator.identificator": "5dbca334-9ce8-4ebe-80c3-01c01dd1844f-b25kZXJkZWVsI0FhbnZ1bGxlbmRlR2VvbWV0cmll"
                },
                "AIMDBStatus.isActief": True,
                "AbstracteAanvullendeGeometrie.naam": "A3N13.9.M.ANPR",
                "geo:Geometrie.log": [
                    {
                        "geo:DtcLog.geometrie": {
                            "geo:DtuGeometrie.punt": "POINT Z (0 0 0)"
                        },
                        "geo:DtcLog.niveau": "https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/-1"
                    },
                    {
                        "geo:DtcLog.bron": "https://geo.data.wegenenverkeer.be/id/concept/KlLogBron/manueel",
                        "geo:DtcLog.geometrie": {
                            "geo:DtuGeometrie.polygoon": "POLYGON Z ((164980.5 172859.4 0,165004.77 172855.89 0,165003 172849.2 0,164980.5 172859.4 0))"
                        },
                        "geo:DtcLog.gaVersie": "",
                        "geo:DtcLog.overerving": [],
                        "geo:DtcLog.nauwkeurigheid": "",
                        "geo:DtcLog.niveau": "https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/0"
                    }
                ],
                "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-ontwerp",
                "AbstracteAanvullendeGeometrie.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#AanvullendeGeometrie"
            }]

    def set_up_assets(self, cursor):
        assets = [{
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
            "@id": "https://data.awvvlaanderen.be/id/asset/000ad2af-e393-4c45-b54f-b0d94524b1e1-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "000ad2af-e393-4c45-b54f-b0d94524b1e1-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV"
            },
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
            "AIMObject.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
            "AIMObject.notitie": "",
            "AIMNaamObject.naam": "VCN-ANT-CS01.Gi4.0.15",
            "AIMDBStatus.isActief": True,
        },
            {
                "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
                "@id": "https://data.awvvlaanderen.be/id/asset/00000453-56ce-4f8b-af44-960df526cb30-bGdjOmluc3RhbGxhdGllI0thc3Q",
                "NaampadObject.naampad": "057A5/KAST",
                "AIMObject.notitie": "",
                "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
                "AIMDBStatus.isActief": True,
                "AIMObject.assetId": {
                    "DtcIdentificator.identificator": "00000453-56ce-4f8b-af44-960df526cb30-bGdjOmluc3RhbGxhdGllI0thc3Q",
                    "DtcIdentificator.toegekendDoor": "AWV"
                },
                "AIMNaamObject.naam": "KAST",
                "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik"
            },
            {
                "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#AanvullendeGeometrie",
                "@id": "https://data.awvvlaanderen.be/id/asset/5dbca334-9ce8-4ebe-80c3-01c01dd1844f-b25kZXJkZWVsI0FhbnZ1bGxlbmRlR2VvbWV0cmll",
                "loc:Locatie.geometrie": "POINT Z (0 0 0)",
                "loc:Locatie.omschrijving": "test omschrijving",
                "AbstracteAanvullendeGeometrie.assetId": {
                    "DtcIdentificator.toegekendDoor": "AWV",
                    "DtcIdentificator.identificator": "5dbca334-9ce8-4ebe-80c3-01c01dd1844f-b25kZXJkZWVsI0FhbnZ1bGxlbmRlR2VvbWV0cmll"
                },
                "AIMDBStatus.isActief": True,
                "geo:Geometrie.log": [
                    {
                        "geo:DtcLog.geometrie": {
                            "geo:DtuGeometrie.punt": "POINT Z (0 0 0)"
                        },
                        "geo:DtcLog.niveau": "https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/-1"
                    }
                ],
                "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-ontwerp",
                "AbstracteAanvullendeGeometrie.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#AanvullendeGeometrie"
            }]
        self.assets_syncer.update_assets(assets_dicts=assets, cursor=cursor)

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
        },
            {
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
            },
            {
                "_type": "onderdeeltype",
                "uuid": "afdeacf2-c21a-4ac4-9ee7-70bebe794638",
                "createdOn": "2022-06-13T14:27:10.596+02:00",
                "modifiedOn": "2022-06-14T16:15:56.578+02:00",
                "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#AanvullendeGeometrie",
                "korteUri": "onderdeel#AanvullendeGeometrie",
                "afkorting": "AanvullendeGeometrie",
                "naam": "Aanvullende geometrie",
                "actief": True,
                "definitie": "Beschrijft een geometrie die aanvullend is bij de de werking van een asset maar beschrijft niet de asset zelf, bv. een detailplan of een werkingsgebied met een specifieke locatie, enz...De aanvullende geometrie kan al dan niet een bijlage bevatten."
            }]
        self.assettypes_syncer.update_assettypes(assettypes_dicts=assettypes)
