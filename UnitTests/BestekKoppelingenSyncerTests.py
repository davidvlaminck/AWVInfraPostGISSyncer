import datetime
from unittest import TestCase

from psycopg2 import connect, tz
from AssetSyncer import AssetSyncer
from AssetTypeSyncer import AssetTypeSyncer
from BestekKoppelingSyncer import BestekKoppelingSyncer
from BestekSyncer import BestekSyncer
from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager


class BestekKoppelingenSyncerTests(TestCase):
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

        self.assettypes_syncer = AssetTypeSyncer(postGIS_connector=self.connector,
                                                 emInfraImporter=self.eminfra_importer)
        self.assets_syncer = AssetSyncer(postGIS_connector=self.connector,
                                         em_infra_importer=self.eminfra_importer)
        self.bestekken_syncer = BestekSyncer(postGIS_connector=self.connector,
                                             em_infra_importer=self.eminfra_importer)
        self.bestekkoppelingen_syncer = BestekKoppelingSyncer(postGIS_connector=self.connector,
                                                              em_infra_importer=self.eminfra_importer)

    def test_update_bestekkoppelingen(self):
        self.setup()

        self.set_up_data()

        create_bestekkoppeling_query = "INSERT INTO bestekkoppelingen (assetUuid, bestekUuid, startDatum, eindDatum, koppelingStatus) " \
                                 "VALUES ('0000da03-06f3-4a22-a609-d82358c62273', '89d95cfa-0bf7-4ccb-af7f-cf9ba9d52da6'," \
                                 "'2017-01-15T00:00:00.000+01:00', NULL, 'ACTIEF')"
        select_bestekkoppeling_query = "SELECT startDatum FROM bestekkoppelingen WHERE assetUuid = '{assetUuid}' and bestekUuid = '{bestekUuid}'"
        count_bestekkoppeling_query = "SELECT count(*) FROM bestekkoppelingen"
        cursor = self.connector.connection.cursor()
        cursor.execute(create_bestekkoppeling_query)

        with self.subTest('date check the first bestekkoppeling created'):
            cursor.execute(select_bestekkoppeling_query.replace('{assetUuid}', '0000da03-06f3-4a22-a609-d82358c62273')
                           .replace('{bestekUuid}', '89d95cfa-0bf7-4ccb-af7f-cf9ba9d52da6'))
            result = cursor.fetchone()[0]
            self.assertEqual(datetime.datetime(year=2017, month=1, day=15, tzinfo=tz.FixedOffsetTimezone(60)), result)
        with self.subTest('number of bestekkoppelingen before update'):
            cursor.execute(count_bestekkoppeling_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        bestekkoppelingen = [{
            "startDatum": "2021-01-14T00:00:00.000+01:00",
            "eindDatum": "2025-01-06T23:59:59.000+01:00",
            "bestekRef": {
                "uuid": "97f240c4-d788-427d-8215-d1880023cc08",
                "awvId": "75132a25-cfc0-3a33-a250-a3647ede8914",
                "eDeltaDossiernummer": "VWT-CEW-2020-009-2",
                "eDeltaBesteknummer": "VWT-CEW-2020-009",
                "type": "PERCEEL",
                "aannemerNaam": "Etablissements Paque, Yvan",
                "aannemerReferentie": "0412815271",
                "links": [
                    {
                        "rel": "self",
                        "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/97f240c4-d788-427d-8215-d1880023cc08"
                    }
                ]
            },
            "status": "ACTIEF"
        },
            {
                "startDatum": "2011-06-01T00:00:00.000+02:00",
                "bestekRef": {
                    "uuid": "c8392f7a-db33-47f1-881f-81431dd9d13c",
                    "nummer": "AWV-EM",
                    "awvId": "cfd0a50e-8e79-3ad4-95c4-f9ce909819dd",
                    "eDeltaDossiernummer": "INTERN-112",
                    "eDeltaBesteknummer": "AWV-EW Limburg",
                    "lot": "LIMBURG",
                    "type": "INTERN",
                    "aannemerNaam": "Permanentie sectie EW AWV Limburg",
                    "aannemerReferentie": "1MD8GYA",
                    "links": [
                        {
                            "rel": "self",
                            "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/c8392f7a-db33-47f1-881f-81431dd9d13c"
                        }
                    ]
                },
                "status": "ACTIEF"
            },
            {
                "startDatum": "2017-01-14T00:00:00.000+01:00",
                "eindDatum": "2021-01-13T23:59:59.000+01:00",
                "bestekRef": {
                    "uuid": "89d95cfa-0bf7-4ccb-af7f-cf9ba9d52da6",
                    "nummer": "1M3D8N/15/02",
                    "awvId": "380b6373-3e27-3e3e-a150-76a9ae0704e1",
                    "eDeltaDossiernummer": "INTERN-1937",
                    "eDeltaBesteknummer": "1M3D8N/15/02",
                    "lot": "P2 : WL",
                    "type": "INTERN",
                    "aannemerNaam": "Yvan Paque SA",
                    "aannemerReferentie": "0412815271",
                    "links": [
                        {
                            "rel": "self",
                            "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/89d95cfa-0bf7-4ccb-af7f-cf9ba9d52da6"
                        }
                    ]
                },
                "status": "INACTIEF"
            }
        ]
        self.bestekkoppelingen_syncer.update_bestekkoppelingen_by_asset_uuids(
            asset_uuids=['0000da03-06f3-4a22-a609-d82358c62273'],
            bestek_koppelingen_dicts=bestekkoppelingen)

        with self.subTest('date check after the first bestekkoppeling updated'):
            cursor.execute(select_bestekkoppeling_query.replace('{assetUuid}', '0000da03-06f3-4a22-a609-d82358c62273')
                           .replace('{bestekUuid}', '89d95cfa-0bf7-4ccb-af7f-cf9ba9d52da6'))
            result = cursor.fetchone()[0]
            self.assertEqual(datetime.datetime(year=2017, month=1, day=14, tzinfo=tz.FixedOffsetTimezone(60)), result)
        with self.subTest('date check after new bestekkoppelingen created'):
            cursor.execute(select_bestekkoppeling_query.replace('{assetUuid}', '0000da03-06f3-4a22-a609-d82358c62273')
                           .replace('{bestekUuid}', 'c8392f7a-db33-47f1-881f-81431dd9d13c'))
            result = cursor.fetchone()[0]
            self.assertEqual(datetime.datetime(year=2011, month=6, day=1, tzinfo=tz.FixedOffsetTimezone(120)), result)
        with self.subTest('number of bestekkoppelingen after update'):
            cursor.execute(count_bestekkoppeling_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)

    def test_sync_bestekkoppelingen_without_existing_asset(self):
        return NotImplementedError

    def test_sync_bestekkoppelingen_without_existing_bestek(self):
        return NotImplementedError

    def set_up_data(self):
        self.assettypes_syncer.update_assettypes([{
            "_type": "installatietype",
            "uuid": "4dfad588-277c-480f-8cdc-0889cfaf9c78",
            "createdOn": "2019-01-16T14:13:06.708+01:00",
            "modifiedOn": "2022-07-13T16:40:16.499+02:00",
            "uri": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
            "korteUri": "lgc:installatie#VPLMast",
            "afkorting": "VPLMast",
            "naam": "Lichtmast wegverlichting",
            "actief": True,
            "definitie": "lichtmast wegverlichting"
        }])
        self.assets_syncer.update_assets([{
            "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
            "@id": "https://data.awvvlaanderen.be/id/asset/0000da03-06f3-4a22-a609-d82358c62273-bGdjOmluc3RhbGxhdGllI1ZQTE1hc3Q",
            "lgc:VPLMast.risicovollePaal": False,
            "ins:VPLMast.toestandBouten": "niet gekend",
            "ins:EMObject.interneRoestvorming": "niet gekend",
            "NaampadObject.naampad": "G0632/G0632.WV/018",
            "tz:Schadebeheerder.schadebeheerder": {
                "tz:DtcBeheerder.naam": "District Centraal - Limburg",
                "tz:DtcBeheerder.referentie": "720"
            },
            "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
            "ins:VPLMast.toestandPaal": "niet gekend",
            "ins:EMObject.toestandVerlichtingstoestellen": "niet gekend",
            "ond:EMObject.aantalLampenVervangen": 0,
            "ins:EMObject.externeRoestvorming": "niet gekend",
            "lgc:VPLMast.bevestigingswijzeMeerdereToestellen": "niet van toepassing",
            "AIMObject.notitie": "",
            "AIMObject.assetId": {
                "DtcIdentificator.toegekendDoor": "AWV",
                "DtcIdentificator.identificator": "0000da03-06f3-4a22-a609-d82358c62273-bGdjOmluc3RhbGxhdGllI1ZQTE1hc3Q"
            },
            "lgc:VPLMast.lichtmastBuitenGebruik": False,
            "lgc:VPLMast.datumLichtmastGeschilderd": "1900-01-01",
            "loc:Locatie.omschrijving": "",
            "ins:VPLMast.toestandDeurtje": "niet gekend",
            "lgc:VPLMast.aantalArmen": "1 - alleen mogelijk voor lichtmast 'M', 'MS', 'B', 'BS', 'K' of 'KS'",
            "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
            "loc:Locatie.geometrie": "",
            "AIMDBStatus.isActief": True,
            "ond:EMObject.aantalZekeringenVervangen": 0,
            "AIMNaamObject.naam": "018",
            "ins:EMObject.directGevaar": False,
            "lgc:EMObject.armatuurkleur": "7038",
            "tz:Toezicht.toezichter": {
                "tz:DtcToezichter.email": "robin.lux@mow.vlaanderen.be",
                "tz:DtcToezichter.voornaam": "Robin",
                "tz:DtcToezichter.gebruikersnaam": "luxro",
                "tz:DtcToezichter.naam": "Lux"
            },
            "lgc:EMObject.verlichtingstoestelSysteemvermogen": 120,
            "lgc:VPLMast.lichtmastType": "M - Metalen paal met arm",
            "ond:EMObject.aantalLamphoudersVervangen": 0,
            "lgc:EMObject.vsaType": "elektromagnetisch",
            "lgc:EMObject.verlichtingstoestelMerkEnType": "Philips Iridium",
            "ond:VPLMast.deurtjeVervangen": False,
            "ond:EMObject.aantalVsaVervangen": 0,
            "lgc:VPLMast.armlengte": [
                "3,2 - Enkel voor lichtmast types 'M', 'MS', 'B', 'BS', 'K' of 'KS'"
            ],
            "lgc:EMObject.aantalVerlichtingstoestellen": 1,
            "loc:Locatie.puntlocatie": {
                "loc:3Dpunt.puntgeometrie": {
                    "loc:DtcCoord.lambert72": {
                        "loc:DtcCoordLambert72.ycoordinaat": 177341.7,
                        "loc:DtcCoordLambert72.xcoordinaat": 219188.2,
                        "loc:DtcCoordLambert72.zcoordinaat": 0
                    }
                },
                "loc:DtcPuntlocatie.precisie": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatiePrecisie/meter",
                "loc:DtcPuntlocatie.bron": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatieBron/manueel",
                "loc:DtcPuntlocatie.adres": {
                    "loc:DtcAdres.straat": "Trekschurenstraat",
                    "loc:DtcAdres.nummer": "286",
                    "loc:DtcAdres.postcode": "3500",
                    "loc:DtcAdres.provincie": "Limburg",
                    "loc:DtcAdres.gemeente": "Hasselt",
                    "loc:DtcAdres.bus": ""
                },
                "loc:DtcPuntlocatie.weglocatie": {
                    "loc:DtcWeglocatie.ident8": "A0131912",
                    "loc:DtcWeglocatie.straatnaam": "A0131911",
                    "loc:DtcWeglocatie.ident2": "A13",
                    "loc:DtcWeglocatie.referentiepaalAfstand": 258,
                    "loc:DtcWeglocatie.gemeente": "Hasselt",
                    "loc:DtcWeglocatie.referentiepaalOpschrift": 0
                }
            },
            "lgc:VPLMast.ralKleurVplmast": "7038",
            "lgc:VPLMast.paalhoogte": "12,50 - niet voor type lichtmast 'K' en 'KS'",
            "lgc:EMObject.verlichtingstype": "opafrit",
            "ins:VPLMast.toestandFunderingVplmast": "niet gekend",
            "lgc:EMObject.ledVerlichting": False,
            "lgc:VPLMast.redenLichtmastBuitenGebruik": "niet van toepassing",
            "ond:EMObject.aantalStartersVervangen": 0,
            "ond:EMObject.aantalKlemmenblokkenVervangen": 0,
            "lgc:EMObject.geschilderd": True,
            "ins:EMObject.nummerLeesbaar": "niet gekend",
            "lgc:EMObject.vsaSperfilter": False,
            "lgc:EMObject.lampType": "NaHP-T-100",
            "lgc:VPLMast.datumLichtmastVernieuwd": "1900-01-01",
            "tz:Toezicht.toezichtgroep": {
                "tz:DtcToezichtGroep.naam": "AWV_EW_LB",
                "tz:DtcToezichtGroep.referentie": "AWV_EW_LB"
            }
        }])
        self.bestekken_syncer.update_bestekken([{
            "uuid": "97f240c4-d788-427d-8215-d1880023cc08",
            "awvId": "75132a25-cfc0-3a33-a250-a3647ede8914",
            "eDeltaDossiernummer": "VWT-CEW-2020-009-2",
            "eDeltaBesteknummer": "VWT-CEW-2020-009",
            "type": "PERCEEL",
            "aannemerNaam": "Etablissements Paque, Yvan",
            "aannemerReferentie": "0412815271",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/97f240c4-d788-427d-8215-d1880023cc08"
                },
                {
                    "rel": "created-by",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten"
                },
                {
                    "rel": "modified-by",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten"
                }
            ]
        }, {
            "uuid": "c8392f7a-db33-47f1-881f-81431dd9d13c",
            "nummer": "AWV-EM",
            "awvId": "cfd0a50e-8e79-3ad4-95c4-f9ce909819dd",
            "eDeltaDossiernummer": "INTERN-112",
            "eDeltaBesteknummer": "AWV-EW Limburg",
            "lot": "LIMBURG",
            "type": "INTERN",
            "aannemerNaam": "Permanentie sectie EW AWV Limburg",
            "aannemerReferentie": "1MD8GYA",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/c8392f7a-db33-47f1-881f-81431dd9d13c"
                },
                {
                    "rel": "created-by",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten"
                },
                {
                    "rel": "modified-by",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten"
                }
            ]
        }, {
            "uuid": "89d95cfa-0bf7-4ccb-af7f-cf9ba9d52da6",
            "nummer": "1M3D8N/15/02",
            "awvId": "380b6373-3e27-3e3e-a150-76a9ae0704e1",
            "eDeltaDossiernummer": "INTERN-1937",
            "eDeltaBesteknummer": "1M3D8N/15/02",
            "lot": "P2 : WL",
            "type": "INTERN",
            "aannemerNaam": "Yvan Paque SA",
            "aannemerReferentie": "0412815271",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/89d95cfa-0bf7-4ccb-af7f-cf9ba9d52da6"
                },
                {
                    "rel": "created-by",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten"
                },
                {
                    "rel": "modified-by",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten"
                }
            ]
        }])
