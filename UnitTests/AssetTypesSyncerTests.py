from unittest import TestCase

from psycopg2 import connect

from AgentSyncer import AgentSyncer
from AssetTypeSyncer import AssetTypeSyncer
from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager


class AssetTypesSyncerTests(TestCase):
    def setup(self):
        conn = connect(database="postgres", user='postgres', password='admin', host='127.0.0.1', port='5432')
        conn.autocommit = True

        cursor = conn.cursor()
        cursor.execute('DROP database unittests;')
        cursor.execute('CREATE database unittests;')

        conn.close()

        self.connector = PostGISConnector(host="127.0.0.1", user="postgres", password="admin", port="5432", database="unittests")
        self.connector.set_up_tables('../setup_tables_querys.txt')
        settings_manager = SettingsManager(settings_path='C:\\resources\\settings_AwvinfraPostGISSyncer.json')

        requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
        request_handler = RequestHandler(requester)
        self.eminfra_importer = EMInfraImporter(request_handler)

        self.assettypes_syncer = AssetTypeSyncer(postGIS_connector=self.connector, emInfraImporter=self.eminfra_importer)

    def test_update_assettypes(self):
        self.setup()

        create_assettype_query = "INSERT INTO assettypes (uuid, naam, label, uri, definitie, actief) " \
                                 "VALUES ('51a08ba4-6657-43a8-b45a-bcde1af7c0c8', 'TLC-FI poort', 'TLCfiPoort', " \
                                 "'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#TLCfiPoort', " \
                                 "'Functionele software component die een TLC-FI interface aanbiedt waardoor data kan uitgewisseld worden voor intelligente verkeersregelaars.'," \
                                 "true)"
        select_assettype_query = "SELECT naam FROM assettypes WHERE uuid = '{uuid}'"
        count_assettype_query = "SELECT count(*) FROM assettypes"
        cursor = self.connector.connection.cursor()
        cursor.execute(create_assettype_query)

        with self.subTest('name check the first agent created'):
            cursor.execute(select_assettype_query.replace('{uuid}', '51a08ba4-6657-43a8-b45a-bcde1af7c0c8'))
            result = cursor.fetchone()[0]
            self.assertEqual('TLC-FI poort', result)
        with self.subTest('number of agents before update'):
            cursor.execute(count_assettype_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        assettypes = [{
            "_type": "onderdeeltype",
            "uuid": "51a08ba4-6657-43a8-b45a-bcde1af7c0c8",
            "createdOn": "2021-07-20T14:34:40.810+02:00",
            "modifiedOn": "2022-06-13T14:27:13.947+02:00",
            "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#TLCfiPoort",
            "korteUri": "onderdeel#TLCfiPoort",
            "afkorting": "TLCfiPoort",
            "naam": "TLC-FI poort changed",
            "actief": True,
            "definitie": "Functionele software component die een TLC-FI interface aanbiedt waardoor data kan uitgewisseld worden voor intelligente verkeersregelaars.",
            "links": [
                {
                    "rel": "self",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/onderdeeltypes/51a08ba4-6657-43a8-b45a-bcde1af7c0c8"
                },
                {
                    "rel": "kenmerktypes",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/onderdeeltypes/51a08ba4-6657-43a8-b45a-bcde1af7c0c8/kenmerktypes"
                },
                {
                    "rel": "created-by",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten/cbb6bd3b-33af-4bba-acdd-84ba0dfdf1e3"
                },
                {
                    "rel": "modified-by",
                    "href": "https://apps.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten/cbb6bd3b-33af-4bba-acdd-84ba0dfdf1e3"
                }
            ]
        },
            {
                "_type": "onderdeeltype",
                "uuid": "5ce8a47b-805c-4af9-b0a3-6d7fa32574c9",
                "createdOn": "2021-07-20T14:34:40.787+02:00",
                "modifiedOn": "2022-06-13T14:27:13.716+02:00",
                "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#ConstructieSokkel",
                "korteUri": "onderdeel#ConstructieSokkel",
                "afkorting": "ConstructieSokkel",
                "naam": "Constructie sokkel",
                "actief": True,
                "definitie": "Betonnen zool die het object dat erop rust verhoogt of dat dient om een structuur op een goede manier te kunnen opleggen/verbinden met de fundering.",
                "links": [
                    {
                        "rel": "self",
                        "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/onderdeeltypes/5ce8a47b-805c-4af9-b0a3-6d7fa32574c9"
                    },
                    {
                        "rel": "kenmerktypes",
                        "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/onderdeeltypes/5ce8a47b-805c-4af9-b0a3-6d7fa32574c9/kenmerktypes"
                    },
                    {
                        "rel": "created-by",
                        "href": "https://apps.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten/cbb6bd3b-33af-4bba-acdd-84ba0dfdf1e3"
                    },
                    {
                        "rel": "modified-by",
                        "href": "https://apps.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten/cbb6bd3b-33af-4bba-acdd-84ba0dfdf1e3"
                    }
                ]
            },
            {
                "_type": "installatietype",
                "uuid": "d9e315a7-6f0d-4a1c-b77c-e56fba983bec",
                "createdOn": "2019-01-16T14:13:16.538+01:00",
                "modifiedOn": "2022-04-29T13:47:10.169+02:00",
                "uri": "https://lgc.data.wegenenverkeer.be/ns/installatie#Z30Paal",
                "korteUri": "lgc:installatie#Z30Paal",
                "afkorting": "Z30Paal",
                "naam": "Zone30 paal",
                "actief": True,
                "definitie": "Dynamische borden : individueel zone30 bord met bijhorende paal, subonderdeel van zone30-installatie",
                "links": [
                    {
                        "rel": "self",
                        "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/installatietypes/d9e315a7-6f0d-4a1c-b77c-e56fba983bec"
                    },
                    {
                        "rel": "kenmerktypes",
                        "href": "https://apps.mow.vlaanderen.be/eminfra/core/api/installatietypes/d9e315a7-6f0d-4a1c-b77c-e56fba983bec/kenmerktypes"
                    },
                    {
                        "rel": "created-by",
                        "href": "https://apps.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten/89b7697e-adce-4017-9f41-9db1c4c23097"
                    },
                    {
                        "rel": "modified-by",
                        "href": "https://apps.mow.vlaanderen.be/eminfra/identiteit/api/identiteiten/52a8199b-7d1f-4f03-8379-e8e2f2255049"
                    }
                ]
            }]
        self.assettypes_syncer.update_assettypes(assettypes_dicts=assettypes)

        with self.subTest('name check after the first assettype updated'):
            cursor.execute(select_assettype_query.replace('{uuid}', '51a08ba4-6657-43a8-b45a-bcde1af7c0c8'))
            result = cursor.fetchone()[0]
            self.assertEqual('TLC-FI poort changed', result)
        with self.subTest('name check after new assettypes created'):
            cursor.execute(select_assettype_query.replace('{uuid}', '5ce8a47b-805c-4af9-b0a3-6d7fa32574c9'))
            result = cursor.fetchone()[0]
            self.assertEqual('Constructie sokkel', result)
        with self.subTest('number of assettypes after update'):
            cursor.execute(count_assettype_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)
