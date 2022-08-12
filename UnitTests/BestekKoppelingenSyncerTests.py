import datetime
from unittest import TestCase

import psycopg2
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
        cursor.execute('DROP database IF EXISTS unittests;')
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

        self.bestekkoppelingen_syncer.update_bestekkoppelingen_by_asset_uuids(cursor=cursor,
            asset_uuids=['0000da03-06f3-4a22-a609-d82358c62273'],
            bestek_koppelingen_dicts_list=[bestekkoppelingen])
        self.connector.connection.commit()

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
        self.setup()

        self.set_up_data_bestekken()

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
        cursor = self.connector.connection.cursor()
        with self.assertRaises(psycopg2.Error) as exc:
            self.bestekkoppelingen_syncer.update_bestekkoppelingen_by_asset_uuids(cursor=cursor,
                asset_uuids=['0000da03-06f3-4a22-a609-d82358c62273'],
                bestek_koppelingen_dicts_list=[bestekkoppelingen])
        self.assertEqual(exc.exception.pgerror.split('\n')[0],
                         'ERROR:  insert or update on table "bestekkoppelingen" violates foreign key constraint "bestekkoppelingen_assets_fkey"')

    def test_sync_bestekkoppelingen_without_existing_bestek(self):
        self.setup()

        self.set_up_data_assettypes()
        self.set_up_data_assets(self.connector.connection.cursor())

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
        cursor = self.connector.connection.cursor()
        with self.assertRaises(psycopg2.Error) as exc:
            self.bestekkoppelingen_syncer.update_bestekkoppelingen_by_asset_uuids(cursor=cursor,
                asset_uuids=['0000da03-06f3-4a22-a609-d82358c62273'],
                bestek_koppelingen_dicts_list=[bestekkoppelingen])
        self.assertEqual(exc.exception.pgerror.split('\n')[0],
                         'ERROR:  insert or update on table "bestekkoppelingen" violates foreign key constraint "bestekkoppelingen_bestekken_fkey"')

    def test_sync_bestekkoppelingen(self):
        self.setup()

        self.set_up_data_bestekken()
        self.set_up_data_assettypes()
        self.set_up_data_multiple_assets(cursor=self.connector.connection.cursor())

        self.bestekkoppelingen_syncer.sync_bestekkoppelingen(2)

    def set_up_data_assettypes(self):
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

    def set_up_data_assets(self, cursor):
        self.assets_syncer.update_assets(cursor=cursor, assets_dicts=[{
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

    def set_up_data_bestekken(self):
        self.bestekken_syncer.update_bestekken([{
            "uuid": "7b9ecee2-4e76-43f9-a17c-43372eb0b7b9",
            "nummer": "AWV-EM",
            "awvId": "904f84c8-51e9-3f10-ba2c-2bb4a629d6f4",
            "eDeltaDossiernummer": "INTERN-150",
            "eDeltaBesteknummer": "AWV-EW West-Vlaanderen",
            "lot": "WV",
            "type": "INTERN",
            "aannemerNaam": "Permanentie sectie EW AWV West-Vlaanderen",
            "aannemerReferentie": "1MD8JWA",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/7b9ecee2-4e76-43f9-a17c-43372eb0b7b9"
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
            "uuid": "e4aa55ea-b2e4-4e5b-9e34-c9018644b039",
            "nummer": "1M3D8N/15/02",
            "awvId": "a3440b33-662c-349f-be32-c5fb4e1d4e93",
            "eDeltaDossiernummer": "MDN/58-5",
            "eDeltaBesteknummer": "MDN/58",
            "lot": "P5 : WWV",
            "type": "PERCEEL",
            "aannemerNaam": "VERKEER SIGNALISATIE EN ELEKTRONIKA",
            "aannemerReferentie": "0422957216",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/e4aa55ea-b2e4-4e5b-9e34-c9018644b039"
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
            "uuid": "063a70be-e291-4e39-a75e-dc533b25eec0",
            "awvId": "b65220f7-8701-340a-9de7-b0a632050891",
            "eDeltaDossiernummer": "VWT-CEW-2020-009-5",
            "eDeltaBesteknummer": "VWT-CEW-2020-009",
            "type": "PERCEEL",
            "aannemerNaam": "VERKEER SIGNALISATIE EN ELEKTRONIKA",
            "aannemerReferentie": "0422957216",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/063a70be-e291-4e39-a75e-dc533b25eec0"
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
            "uuid": "90bddb11-419b-4aff-a44a-fd5db4b2ee00",
            "awvId": "181e10ca-c806-33dd-83a4-d403bd199a04",
            "eDeltaDossiernummer": "VWT-CEW-2020-009-1",
            "eDeltaBesteknummer": "VWT-CEW-2020-009",
            "type": "PERCEEL",
            "aannemerNaam": "TM ANTWERPEN VERLICHT",
            "aannemerReferentie": "0761415554",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/90bddb11-419b-4aff-a44a-fd5db4b2ee00"
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
            "uuid": "756cd2c2-3071-4bd4-8dc7-d862497bd0f5",
            "nummer": "AWV-EM",
            "awvId": "a82dc22d-5558-3a76-8d21-cee77fc30822",
            "eDeltaDossiernummer": "INTERN-115",
            "eDeltaBesteknummer": "AWV-EW Antwerpen",
            "lot": "ANTWERPEN",
            "type": "INTERN",
            "aannemerNaam": "Permanentie sectie EW AWV Antwerpen",
            "aannemerReferentie": "1MD8EZE",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/756cd2c2-3071-4bd4-8dc7-d862497bd0f5"
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
            "uuid": "4b093e67-8a26-4192-a3ac-92a2efa29af7",
            "nummer": "1M3D8N/15/02",
            "awvId": "20948d35-12d5-3cbb-bfa9-8ea7328c3b2f",
            "eDeltaDossiernummer": "MDN/58-1",
            "eDeltaBesteknummer": "MDN/58",
            "lot": "P1 : WA",
            "type": "PERCEEL",
            "aannemerNaam": "THV OV Antwerpen",
            "aannemerReferentie": "0670537244",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/4b093e67-8a26-4192-a3ac-92a2efa29af7"
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
            "uuid": "a73aa0cf-c37b-4aa1-b73a-29baef77b0de",
            "awvId": "137a30e2-cd9e-3490-9851-fad3532880a0",
            "eDeltaDossiernummer": "VWT/CEW/2020/009-3",
            "eDeltaBesteknummer": "VWT/CEW/2020/009",
            "type": "PERCEEL",
            "aannemerNaam": "TRAFIROAD",
            "aannemerReferentie": "0418384358",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/a73aa0cf-c37b-4aa1-b73a-29baef77b0de"
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
            "uuid": "a62935a9-a41a-440d-a604-7ec9ce7783c9",
            "nummer": "AWV-EM",
            "awvId": "0ad0907b-84e6-32d7-889a-3f69b1b9c094",
            "eDeltaDossiernummer": "INTERN-214",
            "eDeltaBesteknummer": "AWV-EW Oost-Vlaanderen",
            "lot": "OV",
            "type": "INTERN",
            "aannemerNaam": "Permanentie sectie EW AWV Oost-Vlaanderen",
            "aannemerReferentie": "1MD8HU",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/a62935a9-a41a-440d-a604-7ec9ce7783c9"
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
            "uuid": "d8432ecb-d5d9-4945-97e9-48718d867e18",
            "nummer": "1M3D8N/16/07",
            "awvId": "3e1c5682-b851-3bd6-a653-66d7e775366d",
            "eDeltaDossiernummer": "MDN/60",
            "eDeltaBesteknummer": "1M3D8N/16/07",
            "lot": "P3 : WOV",
            "type": "OVERHEIDSOPDRACHT",
            "aannemerNaam": "TRAFIROAD",
            "aannemerReferentie": "0418384358",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/d8432ecb-d5d9-4945-97e9-48718d867e18"
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
            "uuid": "cbcfcf29-86da-4973-8bea-727f814e208a",
            "awvId": "5a87dbda-c43f-3615-a4c7-d3a8466a59fb",
            "eDeltaDossiernummer": "VWT-CEW-2020-009-4",
            "eDeltaBesteknummer": "VWT-CEW-2020-009",
            "type": "PERCEEL",
            "aannemerNaam": "ELEC. D.V.C.",
            "aannemerReferentie": "0460799290",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/cbcfcf29-86da-4973-8bea-727f814e208a"
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
            "uuid": "2cc4d00f-d61d-4f3f-940a-9871a74fccd3",
            "nummer": "AWV-EM",
            "awvId": "4e31a814-5ff4-30d6-84fa-eb070d137ffb",
            "eDeltaDossiernummer": "INTERN-429",
            "eDeltaBesteknummer": "AWV-EW Vlaams-Brabant",
            "lot": "VLAAMS BRA",
            "type": "INTERN",
            "aannemerNaam": "Permanentie sectie EW AWV Vlaams-Brabant",
            "aannemerReferentie": "1MD8FUB",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/bestekrefs/2cc4d00f-d61d-4f3f-940a-9871a74fccd3"
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

    def set_up_data_multiple_assets(self, cursor):
        self.assets_syncer.update_assets(cursor=cursor, assets_dicts=[
            {
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
            },
            {
                "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
                "@id": "https://data.awvvlaanderen.be/id/asset/00013e05-09c8-42de-baf3-a199f9ffb96f-bGdjOmluc3RhbGxhdGllI1ZQTE1hc3Q",
                "lgc:VPLMast.risicovollePaal": False,
                "ins:EMObject.toestandVerlichtingstoestellen": "OK",
                "AIMObject.notitie": "",
                "loc:Locatie.puntlocatie": {
                    "loc:DtcPuntlocatie.adres": {
                        "loc:DtcAdres.gemeente": "Zuienkerke",
                        "loc:DtcAdres.bus": "",
                        "loc:DtcAdres.postcode": "8377",
                        "loc:DtcAdres.provincie": "West-Vlaanderen",
                        "loc:DtcAdres.straat": "Oostendse Steenweg",
                        "loc:DtcAdres.nummer": "66"
                    },
                    "loc:DtcPuntlocatie.weglocatie": {
                        "loc:DtcWeglocatie.straatnaam": "Oostendse Steenweg",
                        "loc:DtcWeglocatie.gemeente": "Zuienkerke",
                        "loc:DtcWeglocatie.referentiepaalAfstand": 13,
                        "loc:DtcWeglocatie.ident2": "N9",
                        "loc:DtcWeglocatie.referentiepaalOpschrift": 0.1,
                        "loc:DtcWeglocatie.ident8": "N0095041"
                    },
                    "loc:3Dpunt.puntgeometrie": {
                        "loc:DtcCoord.lambert72": {
                            "loc:DtcCoordLambert72.ycoordinaat": 216053.3,
                            "loc:DtcCoordLambert72.xcoordinaat": 63661.6,
                            "loc:DtcCoordLambert72.zcoordinaat": 0
                        }
                    },
                    "loc:DtcPuntlocatie.precisie": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatiePrecisie/meter",
                    "loc:DtcPuntlocatie.bron": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatieBron/manueel"
                },
                "lgc:EMObject.aantalTeVerlichtenRijvakkenLed": "niet gekend",
                "lgc:VPLMast.bevestigingswijzeMeerdereToestellen": "niet gekend",
                "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
                "lgc:EMObject.verlichtingstoestelMerkEnType": "Schreder Syntra",
                "ins:EMObject.externeRoestvorming": "OK",
                "lgc:EMObject.overhangLed": "niet gekend",
                "lgc:EMObject.vsaType": "niet gekend",
                "ins:EMObject.nummerLeesbaar": "Ja",
                "lgc:VPLMast.armlengte": [
                    "niet gekend"
                ],
                "tz:Toezicht.toezichter": {
                    "tz:DtcToezichter.gebruikersnaam": "geysenel",
                    "tz:DtcToezichter.naam": "Geysen",
                    "tz:DtcToezichter.voornaam": "Els",
                    "tz:DtcToezichter.email": "els.geysen@mow.vlaanderen.be"
                },
                "lgc:VPLMast.lichtmastBuitenGebruik": False,
                "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
                "ond:EMObject.aantalLampenVervangen": 1,
                "loc:Locatie.geometrie": "",
                "AIMDBStatus.isActief": True,
                "ond:EMObject.aantalZekeringenVervangen": 0,
                "NaampadObject.naampad": "WW0246/WW0246.WV/C11",
                "lgc:EMObject.verlichtingsniveauLed": "niet gekend",
                "ins:EMObject.directGevaar": False,
                "ins:VPLMast.toestandFunderingVplmast": "OK",
                "lgc:VPLMast.aantalArmen": "niet gekend",
                "ins:EMObject.interneRoestvorming": "OK",
                "lgc:EMObject.verlichtingstoestelSysteemvermogen": 120,
                "tz:Schadebeheerder.schadebeheerder": {
                    "tz:DtcBeheerder.referentie": "311",
                    "tz:DtcBeheerder.naam": "District Brugge"
                },
                "AIMNaamObject.naam": "C11",
                "ond:EMObject.aantalLamphoudersVervangen": 0,
                "lgc:EMObject.verlichtingstype": "doorlopende straatverlichting",
                "lgc:VPLMast.leverancier": "niet gekend",
                "ond:VPLMast.deurtjeVervangen": False,
                "ins:VPLMast.toestandDeurtje": "OK",
                "ond:EMObject.aantalVsaVervangen": 0,
                "lgc:EMObject.kleurtemperatuurLed": "niet gekend",
                "lgc:EMObject.aantalVerlichtingstoestellen": 1,
                "tz:Toezicht.toezichtgroep": {
                    "tz:DtcToezichtGroep.naam": "AWV_EW_WV",
                    "tz:DtcToezichtGroep.referentie": "AWV_EW_WV"
                },
                "AIMObject.assetId": {
                    "DtcIdentificator.identificator": "00013e05-09c8-42de-baf3-a199f9ffb96f-bGdjOmluc3RhbGxhdGllI1ZQTE1hc3Q",
                    "DtcIdentificator.toegekendDoor": "AWV"
                },
                "lgc:EMObject.ledVerlichting": False,
                "lgc:VPLMast.redenLichtmastBuitenGebruik": "niet van toepassing",
                "lgc:VPLMast.paalhoogte": "8,00",
                "ond:EMObject.aantalStartersVervangen": 0,
                "lgc:VPLMast.lichtmastType": "RM - Rechte metalen paal",
                "lgc:EMObject.contractnummerLeveringLed": "niet gekend",
                "ond:EMObject.aantalKlemmenblokkenVervangen": 0,
                "lgc:EMObject.geschilderd": False,
                "ins:VPLMast.toestandPaal": "OK",
                "lgc:EMObject.vsaSperfilter": False,
                "lgc:EMObject.lampType": "NaHP-T-100",
                "loc:Locatie.omschrijving": "weg: N009; ident8: N0095041; kilometerpunt: 0,000; zijde weg: Rechts",
                "ins:VPLMast.toestandBouten": "OK"
            },
            {
                "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
                "@id": "https://data.awvvlaanderen.be/id/asset/00023539-f3df-40ed-ad1f-af3c350a7fc6-bGdjOmluc3RhbGxhdGllI1ZQTE1hc3Q",
                "lgc:VPLMast.lichtmastBuitenGebruik": False,
                "lgc:VPLMast.armlengte": [
                    "niet van toepassing - Indien lichtmast type niet gelijk is aan 'M', 'MS', 'B', 'BS', 'K' of 'KS'"
                ],
                "tz:Schadebeheerder.schadebeheerder": {
                    "tz:DtcBeheerder.referentie": "125",
                    "tz:DtcBeheerder.naam": "District Vosselaar"
                },
                "lgc:VPLMast.aantalArmen": "0 - geen armen",
                "lgc:EMObject.verlichtingstoestelSysteemvermogen": 180,
                "tz:Toezicht.toezichtgroep": {
                    "tz:DtcToezichtGroep.referentie": "AWV_EW_AN",
                    "tz:DtcToezichtGroep.naam": "AWV_EW_AN"
                },
                "lgc:EMObject.aantalVerlichtingstoestellen": 1,
                "ins:EMObject.toestandVerlichtingstoestellen": "niet gekend",
                "lgc:VPLMast.risicovollePaal": False,
                "lgc:VPLMast.lichtmastType": "RK - Kreukelpaal",
                "lgc:EMObject.vsaType": "niet gekend",
                "ins:VPLMast.toestandBouten": "niet gekend",
                "ond:EMObject.aantalLampenVervangen": 0,
                "ins:EMObject.externeRoestvorming": "niet gekend",
                "lgc:VPLMast.bevestigingswijzeMeerdereToestellen": "niet van toepassing",
                "AIMObject.notitie": "",
                "AIMNaamObject.naam": "044",
                "ins:VPLMast.toestandDeurtje": "niet gekend",
                "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
                "loc:Locatie.geometrie": "",
                "loc:Locatie.omschrijving": "E34 - COMPLEX - KMP 20 - CABINE HS - KMP 19 TOT 21.2",
                "ins:EMObject.interneRoestvorming": "niet gekend",
                "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
                "loc:Locatie.puntlocatie": {
                    "loc:DtcPuntlocatie.adres": {
                        "loc:DtcAdres.gemeente": "Zoersel",
                        "loc:DtcAdres.bus": "",
                        "loc:DtcAdres.straat": "Den Haan",
                        "loc:DtcAdres.nummer": "7",
                        "loc:DtcAdres.provincie": "Antwerpen",
                        "loc:DtcAdres.postcode": "2980"
                    },
                    "loc:DtcPuntlocatie.weglocatie": {
                        "loc:DtcWeglocatie.gemeente": "Zoersel",
                        "loc:DtcWeglocatie.ident2": "A21",
                        "loc:DtcWeglocatie.referentiepaalOpschrift": 20.5,
                        "loc:DtcWeglocatie.referentiepaalAfstand": 31,
                        "loc:DtcWeglocatie.ident8": "A0210001",
                        "loc:DtcWeglocatie.straatnaam": "A0210001"
                    },
                    "loc:DtcPuntlocatie.precisie": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatiePrecisie/meter",
                    "loc:3Dpunt.puntgeometrie": {
                        "loc:DtcCoord.lambert72": {
                            "loc:DtcCoordLambert72.ycoordinaat": 214899.4,
                            "loc:DtcCoordLambert72.xcoordinaat": 173730.8,
                            "loc:DtcCoordLambert72.zcoordinaat": 0
                        }
                    },
                    "loc:DtcPuntlocatie.bron": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatieBron/manueel"
                },
                "AIMDBStatus.isActief": True,
                "ond:EMObject.aantalZekeringenVervangen": 0,
                "ins:VPLMast.toestandPaal": "niet gekend",
                "lgc:EMObject.lampType": "NaHP-T-150",
                "NaampadObject.naampad": "A1834/A1834.WV/044",
                "ins:EMObject.directGevaar": False,
                "AIMObject.assetId": {
                    "DtcIdentificator.identificator": "00023539-f3df-40ed-ad1f-af3c350a7fc6-bGdjOmluc3RhbGxhdGllI1ZQTE1hc3Q",
                    "DtcIdentificator.toegekendDoor": "AWV"
                },
                "ond:EMObject.aantalLamphoudersVervangen": 0,
                "lgc:VPLMast.leverancier": "niet gekend",
                "lgc:EMObject.verlichtingstoestelMerkEnType": "Philips Iridium",
                "ond:VPLMast.deurtjeVervangen": False,
                "ond:EMObject.aantalVsaVervangen": 0,
                "lgc:EMObject.verlichtingstype": "opafrit",
                "ins:VPLMast.toestandFunderingVplmast": "niet gekend",
                "lgc:VPLMast.redenLichtmastBuitenGebruik": "niet van toepassing",
                "lgc:VPLMast.paalhoogte": "8,00",
                "ond:EMObject.aantalStartersVervangen": 0,
                "tz:Toezicht.toezichter": {
                    "tz:DtcToezichter.voornaam": "Koen",
                    "tz:DtcToezichter.gebruikersnaam": "geeraeko",
                    "tz:DtcToezichter.naam": "Geeraert",
                    "tz:DtcToezichter.email": "koen.geeraert@mow.vlaanderen.be"
                },
                "ond:EMObject.aantalKlemmenblokkenVervangen": 0,
                "lgc:EMObject.geschilderd": False,
                "ins:EMObject.nummerLeesbaar": "niet gekend"
            },
            {
                "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
                "@id": "https://data.awvvlaanderen.be/id/asset/00026034-00a5-4420-9583-8b4295ccf949-bGdjOmluc3RhbGxhdGllI1ZQTE1hc3Q",
                "lgc:VPLMast.risicovollePaal": False,
                "ins:EMObject.externeRoestvorming": "niet gekend",
                "AIMObject.notitie": "",
                "lgc:VPLMast.lichtmastBuitenGebruik": False,
                "lgc:EMObject.verlichtingstoestelMerkEnType": "niet gekend",
                "ins:EMObject.interneRoestvorming": "niet gekend",
                "lgc:EMObject.verlichtingstoestelSysteemvermogen": 180,
                "AIMObject.typeURI": "https://lgc.data.wegenenverkeer.be/ns/installatie#VPLMast",
                "ins:EMObject.directGevaar": False,
                "lgc:VPLMast.aantalArmen": "niet gekend",
                "AIMNaamObject.naam": "C11",
                "lgc:VPLMast.leverancier": "niet gekend",
                "lgc:VPLMast.redenLichtmastBuitenGebruik": "niet van toepassing",
                "lgc:VPLMast.lichtmastType": "niet gekend",
                "lgc:EMObject.vsaType": "niet gekend",
                "lgc:VPLMast.armlengte": [
                    "niet gekend"
                ],
                "ins:VPLMast.toestandBouten": "niet gekend",
                "lgc:VPLMast.bevestigingswijzeMeerdereToestellen": "niet gekend",
                "ins:VPLMast.toestandDeurtje": "niet gekend",
                "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
                "loc:Locatie.geometrie": "",
                "tz:Toezicht.toezichtgroep": {
                    "tz:DtcToezichtGroep.naam": "AWV_EW_OV",
                    "tz:DtcToezichtGroep.referentie": "AWV_EW_OV"
                },
                "AIMObject.assetId": {
                    "DtcIdentificator.toegekendDoor": "AWV",
                    "DtcIdentificator.identificator": "00026034-00a5-4420-9583-8b4295ccf949-bGdjOmluc3RhbGxhdGllI1ZQTE1hc3Q"
                },
                "loc:Locatie.puntlocatie": {
                    "loc:DtcPuntlocatie.adres": {
                        "loc:DtcAdres.provincie": "Oost-Vlaanderen",
                        "loc:DtcAdres.bus": "",
                        "loc:DtcAdres.nummer": "2",
                        "loc:DtcAdres.gemeente": "Lochristi",
                        "loc:DtcAdres.postcode": "9080",
                        "loc:DtcAdres.straat": "Zevestraat"
                    },
                    "loc:3Dpunt.puntgeometrie": {
                        "loc:DtcCoord.lambert72": {
                            "loc:DtcCoordLambert72.ycoordinaat": 195067.1,
                            "loc:DtcCoordLambert72.xcoordinaat": 115544.4,
                            "loc:DtcCoordLambert72.zcoordinaat": 0
                        }
                    },
                    "loc:DtcPuntlocatie.precisie": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatiePrecisie/plus_meter",
                    "loc:DtcPuntlocatie.bron": "https://loc.data.wegenenverkeer.be/id/concept/KlLocatieBron/meettoestel",
                    "loc:DtcPuntlocatie.weglocatie": {
                        "loc:DtcWeglocatie.referentiepaalAfstand": 35,
                        "loc:DtcWeglocatie.referentiepaalOpschrift": 61.7,
                        "loc:DtcWeglocatie.gemeente": "Lochristi",
                        "loc:DtcWeglocatie.straatnaam": "A0140001",
                        "loc:DtcWeglocatie.ident8": "A0140001",
                        "loc:DtcWeglocatie.ident2": "A14"
                    }
                },
                "AIMDBStatus.isActief": True,
                "tz:Toezicht.toezichter": {
                    "tz:DtcToezichter.naam": "Kaya",
                    "tz:DtcToezichter.email": "cenkhan.kaya@mow.vlaanderen.be",
                    "tz:DtcToezichter.voornaam": "Cenk",
                    "tz:DtcToezichter.gebruikersnaam": "kayace"
                },
                "ins:VPLMast.toestandPaal": "niet gekend",
                "tz:Schadebeheerder.schadebeheerder": {
                    "tz:DtcBeheerder.naam": "District St-Niklaas",
                    "tz:DtcBeheerder.referentie": "414"
                },
                "lgc:EMObject.lampType": "NaHP-T-150",
                "ond:VPLMast.deurtjeVervangen": False,
                "NaampadObject.naampad": "WO0166/WO0166.WV/C11",
                "ins:EMObject.toestandVerlichtingstoestellen": "niet gekend",
                "loc:Locatie.omschrijving": "WEG: ; IDENT8: A0140001; KILOMETERPUNT: ; ZIJDE WEG: Z44-MDN60",
                "lgc:EMObject.verlichtingstype": "opafrit",
                "ins:VPLMast.toestandFunderingVplmast": "niet gekend",
                "lgc:EMObject.geschilderd": False,
                "ins:EMObject.nummerLeesbaar": "niet gekend",
                "lgc:EMObject.vsaSperfilter": False,
                "lgc:VPLMast.paalhoogte": "niet gekend"
            }
        ])

    def set_up_data(self):
        self.set_up_data_assettypes()
        self.set_up_data_assets(cursor=self.connector.connection.cursor())
        self.set_up_data_bestekken()
