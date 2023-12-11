from typing import Dict, Generator

import pytest
from psycopg2 import connect

from AssetSyncer import AssetSyncer
from AssetTypeUpdater import AssetTypeUpdater
from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.WeglocatieGewijzigdProcessor import WeglocatieGewijzigdProcessor
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager


@pytest.fixture(scope='module')
def setup():
    settings_manager = SettingsManager(
        settings_path='/home/davidlinux/Documents/AWV/resources/settings_AwvinfraPostGISSyncer.json')
    unittest_db_settings = settings_manager.settings['databases']['unittest']
    unittest_db_settings['database'] = 'unittests'

    connector = PostGISConnector(**unittest_db_settings)

    cursor = connector.main_connection.cursor()

    truncate_queries = """SELECT 'TRUNCATE ' || input_table_name || ' CASCADE;' AS truncate_query FROM(
    SELECT table_schema || '.' || table_name AS input_table_name FROM information_schema.tables
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'views') AND table_schema NOT LIKE 'pg_toast%'
        AND table_name != 'spatial_ref_sys' AND table_type = 'BASE TABLE')
    AS information; """

    cursor.execute(truncate_queries)
    truncate_queries = cursor.fetchall()

    for truncate_query in truncate_queries:
        cursor.execute(truncate_query[0])

    connector.set_up_tables('../setup_tables_querys.sql')

    requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
    request_handler = RequestHandler(requester)
    eminfra_importer = EMInfraImporter(request_handler)

    connection = connector.get_connection()
    assettype_updater = AssetTypeUpdater(postgis_connector=connector, eminfra_importer=eminfra_importer)
    assettype_updater.perform_upsert(connection=connection, object_generator=get_assettypes())

    assets_syncer = AssetSyncer(postgis_connector=connector, eminfra_importer=eminfra_importer)
    _, values = assets_syncer.updater.fill_values_from_object_generator(
        asset_dict_list=[], asset_uuids=[], counter=0, object_generator=get_assets(), values='')
    assets_syncer.updater.perform_insert_update_from_values(connection, insert_only=True, values=values)
    connection.commit()

    return {'connector': connector, 'eminfra_importer': eminfra_importer}


def test_update_weglocaties(setup, subtests):
    eminfra_importer = setup['eminfra_importer']
    connector = setup['connector']
    connection = connector.get_connection()
    processor = WeglocatieGewijzigdProcessor(eminfra_importer=eminfra_importer)
    asset_uuids = ['000ad2af-e393-4c45-b54f-b0d94524b1e1']
    processor.process_dicts(connection=connection, asset_uuids=asset_uuids,
                            asset_dicts=return_new_asset_dicts(asset_uuids))
    connection.commit()

    cursor = connection.cursor()
    with subtests.test(msg='check weglocatie of asset'):
        select_weglocatie_query = "SELECT bron, score, geometrie FROM weglocaties WHERE assetUuid = '{uuid}'"
        cursor.execute(select_weglocatie_query.replace('{uuid}', '000ad2af-e393-4c45-b54f-b0d94524b1e1'))
        result_row = cursor.fetchone()
        assert result_row[0] == 'automatisch'
        assert result_row[1] == '12.782011089369375'
        assert result_row[2] == 'POINT Z(153759.7 211533.4 0)'

    with subtests.test(msg='check wegsegmenten of asset'):
        select_wegsegmenten_query = "SELECT oidn FROM weglocatie_wegsegmenten WHERE assetUuid = '{uuid}'"
        cursor.execute(select_wegsegmenten_query.replace('{uuid}', '000ad2af-e393-4c45-b54f-b0d94524b1e1'))
        results = cursor.fetchall()
        assert results[0][0] == 347253
        assert results[1][0] == 347254

def return_new_asset_dicts(asset_uuids: [str]) -> [Dict]:
    asset_list = [
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
                }, {
                    "wl:DtcWegsegment.oidn": 347254
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
    return [next(asset for asset in asset_list if asset['@id'].split('/')[-1][:36] == uuid) for uuid in asset_uuids]


def get_assets() -> Generator[Dict, None, None]:
    yield from [
        {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
            "@id": "https://data.awvvlaanderen.be/id/asset/000ad2af-e393-4c45-b54f-b0d94524b1e1-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
            "AIMObject.assetId": {
                "DtcIdentificator.identificator": "000ad2af-e393-4c45-b54f-b0d94524b1e1-b25kZXJkZWVsI05ldHdlcmtwb29ydA",
                "DtcIdentificator.toegekendDoor": "AWV",
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
                "DtcIdentificator.toegekendDoor": "AWV",
            },
            "AIMNaamObject.naam": "KAST",
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
        },
        {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#AanvullendeGeometrie",
            "@id": "https://data.awvvlaanderen.be/id/asset/5dbca334-9ce8-4ebe-80c3-01c01dd1844f-b25kZXJkZWVsI0FhbnZ1bGxlbmRlR2VvbWV0cmll",
            "loc:Locatie.geometrie": "POINT Z (0 0 0)",
            "loc:Locatie.omschrijving": "test omschrijving",
            "AbstracteAanvullendeGeometrie.assetId": {
                "DtcIdentificator.toegekendDoor": "AWV",
                "DtcIdentificator.identificator": "5dbca334-9ce8-4ebe-80c3-01c01dd1844f-b25kZXJkZWVsI0FhbnZ1bGxlbmRlR2VvbWV0cmll",
            },
            "AIMDBStatus.isActief": True,
            "geo:Geometrie.log": [
                {
                    "geo:DtcLog.geometrie": {
                        "geo:DtuGeometrie.punt": "POINT Z (0 0 0)"
                    },
                    "geo:DtcLog.niveau": "https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/-1",
                }
            ],
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-ontwerp",
            "AbstracteAanvullendeGeometrie.typeURI": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#AanvullendeGeometrie",
        },
    ]


def get_assettypes() -> Generator[Dict, None, None]:
    yield from [
        {
            "_type": "onderdeeltype",
            "uuid": "6b3dba37-7b73-4346-a264-f4fe5b796c02",
            "createdOn": "2019-12-16T20:33:52.303+01:00",
            "modifiedOn": "2022-06-13T14:27:13.874+02:00",
            "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkpoort",
            "korteUri": "onderdeel#Netwerkpoort",
            "afkorting": "Netwerkpoort",
            "naam": "Netwerkpoort",
            "actief": True,
            "definitie": "De ingang van het toestel samen met component die erop zit,bv. SFP of XFP.",
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
            "definitie": "Installatiekast of Voetpadkast - fysieke behuizing",
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
            "definitie": "Beschrijft een geometrie die aanvullend is bij de de werking van een asset maar beschrijft niet de asset zelf, bv. een detailplan of een werkingsgebied met een specifieke locatie, enz...De aanvullende geometrie kan al dan niet een bijlage bevatten.",
        },
    ]
