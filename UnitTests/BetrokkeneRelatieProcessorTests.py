from unittest import TestCase
from unittest.mock import MagicMock

from psycopg2 import connect

from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.BetrokkeneRelatiesGewijzigdProcessor import BetrokkeneRelatiesGewijzigdProcessor
from Exceptions.AgentMissingError import AgentMissingError
from PostGISConnector import PostGISConnector
from SettingsManager import SettingsManager


class BetrokkeneRelatieProcessorTests(TestCase):
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

    def test_update_betrokkenerelaties(self):
        self.setup()

        cursor = self.connector.connection.cursor()
        self.set_up_agents(cursor)
        self.set_up_assets(cursor)

        processor = BetrokkeneRelatiesGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer,
                                                         connector=self.connector)
        processor.eminfra_importer.import_betrokkenerelaties_from_webservice_by_assetuuids = self.return_betrokkenerelaties

        create_betrokkenerelatie_query = """
        INSERT INTO betrokkeneRelaties (uuid, doelUuid, bronUuid, rol, actief) 
        VALUES ('00000000-0000-0000-1000-000000000000', '10000000-0000-0000-0000-000000000000', 
        '00000000-0000-1000-0000-000000000000', 'toezichter', TRUE)"""
        select_bron_betrokkenerelatie_query = "SELECT bronUuid FROM betrokkeneRelaties WHERE uuid = '{uuid}'"
        select_doel_betrokkenerelatie_query = "SELECT doelUuid FROM betrokkeneRelaties WHERE uuid = '{uuid}'"
        count_betrokkenerelatie_query = "SELECT count(*) FROM betrokkeneRelaties"
        cursor = self.connector.connection.cursor()
        cursor.execute(create_betrokkenerelatie_query)

        with self.subTest('check the betrokkenerelatie created'):
            cursor.execute(
                select_bron_betrokkenerelatie_query.replace('{uuid}', '00000000-0000-0000-1000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('00000000-0000-1000-0000-000000000000', result)
            cursor.execute(
                select_doel_betrokkenerelatie_query.replace('{uuid}', '00000000-0000-0000-1000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('10000000-0000-0000-0000-000000000000', result)
        with self.subTest('number of betrokkenerelaties before update'):
            cursor.execute(count_betrokkenerelatie_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        processor.process(['00000000-0000-1000-0000-000000000000', '00000000-0000-2000-0000-000000000000',
                           '00000000-0000-3000-0000-000000000000'])

        with self.subTest('check after the betrokkenerelatie updated'):
            cursor.execute(
                select_bron_betrokkenerelatie_query.replace('{uuid}', '00000000-0000-0000-1000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('00000000-0000-1000-0000-000000000000', result)
            cursor.execute(
                select_doel_betrokkenerelatie_query.replace('{uuid}', '00000000-0000-0000-1000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('20000000-0000-0000-0000-000000000000', result)
        with self.subTest('check after the betrokkenerelatie created'):
            cursor.execute(
                select_bron_betrokkenerelatie_query.replace('{uuid}', '00000000-0000-0000-2000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('00000000-0000-2000-0000-000000000000', result)
        with self.subTest('number of betrokkenerelaties after update'):
            cursor.execute(count_betrokkenerelatie_query)
            result = cursor.fetchone()[0]
            self.assertEqual(4, result)

    def test_missing_agent(self):
        self.setup()

        cursor = self.connector.connection.cursor()
        self.set_up_one_agent(cursor)
        self.set_up_assets(cursor)

        self.connector.commit_transaction()

        processor = BetrokkeneRelatiesGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer,
                                                         connector=self.connector)
        processor.eminfra_importer.import_betrokkenerelaties_from_webservice_by_assetuuids = self.return_betrokkenerelaties

        with self.assertRaises(AgentMissingError) as exc:
            processor.process(['00000000-0000-1000-0000-000000000000', '00000000-0000-2000-0000-000000000000',
                               '00000000-0000-3000-0000-000000000000'])
        self.assertListEqual(['10000000-0000-0000-0000-000000000000'], exc.exception.args[0])
        self.connector.connection.close()

    def set_up_one_agent(self, cursor):
        cursor.execute("""
        INSERT INTO agents (uuid, naam, actief) 
        VALUES ('20000000-0000-0000-0000-000000000000', 'agent2', TRUE)""")

    def set_up_agents(self, cursor):
        cursor.execute("""
        INSERT INTO agents (uuid, naam, actief) 
        VALUES ('10000000-0000-0000-0000-000000000000', 'agent1', TRUE),
            ('20000000-0000-0000-0000-000000000000', 'agent2', TRUE)""")

    def set_up_assets(self, cursor):
        cursor.execute("""
        INSERT INTO assettypes (uuid, naam, label, uri, actief) 
        VALUES ('00000000-1000-0000-0000-000000000000', 'type1', 'type1', 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#type1', TRUE)""")

        cursor.execute("""
        INSERT INTO assets (uuid, naam, assettype, actief) 
        VALUES ('00000000-0000-1000-0000-000000000000', 'asset1', '00000000-1000-0000-0000-000000000000', TRUE),
        ('00000000-0000-2000-0000-000000000000', 'asset2', '00000000-1000-0000-0000-000000000000', TRUE),
        ('00000000-0000-3000-0000-000000000000', 'asset3', '00000000-1000-0000-0000-000000000000', TRUE)""")

    def return_betrokkenerelaties(self, asset_uuids):
        return [{
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBetrokkene",
            "@id": "https://data.awvvlaanderen.be/id/assetrelatie/00000000-0000-0000-1000-000000000000-b25kZXJkZWVsI0hlZWZ0QmV0cm9ra2VuZQ",
            "RelatieObject.bron": {
                "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkkaart",
                "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-1000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtrYWFydA"
            },
            "RelatieObject.doel": {
                "@type": "http://purl.org/dc/terms/Agent",
                "@id": "https://data.awvvlaanderen.be/id/asset/20000000-0000-0000-0000-000000000000-cHVybDpBZ2VudA"
            },
            "HeeftBetrokkene.rol": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlBetrokkenheidRol/toezichter",
            "HeeftBetrokkene.specifiekeContactinfo": [],
            "AIMDBStatus.isActief": True
        }, {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBetrokkene",
            "@id": "https://data.awvvlaanderen.be/id/assetrelatie/00000000-0000-0000-2000-000000000000-b25kZXJkZWVsI0hlZWZ0QmV0cm9ra2VuZQ",
            "RelatieObject.bron": {
                "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkkaart",
                "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-2000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtrYWFydA"
            },
            "RelatieObject.doel": {
                "@type": "http://purl.org/dc/terms/Agent",
                "@id": "https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-cHVybDpBZ2VudA"
            },
            "HeeftBetrokkene.rol": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlBetrokkenheidRol/toezichter",
            "HeeftBetrokkene.specifiekeContactinfo": [],
            "AIMDBStatus.isActief": True
        }, {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBetrokkene",
            "@id": "https://data.awvvlaanderen.be/id/assetrelatie/00000000-0000-0000-3000-000000000000-b25kZXJkZWVsI0hlZWZ0QmV0cm9ra2VuZQ",
            "RelatieObject.bron": {
                "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Netwerkkaart",
                "@id": "https://data.awvvlaanderen.be/id/asset/00000000-0000-3000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtrYWFydA"
            },
            "RelatieObject.doel": {
                "@type": "http://purl.org/dc/terms/Agent",
                "@id": "https://data.awvvlaanderen.be/id/asset/20000000-0000-0000-0000-000000000000-cHVybDpBZ2VudA"
            },
            "HeeftBetrokkene.rol": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlBetrokkenheidRol/toezichter",
            "HeeftBetrokkene.specifiekeContactinfo": [],
            "AIMDBStatus.isActief": True
        }, {
            "@type": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBetrokkene",
            "@id": "https://data.awvvlaanderen.be/id/assetrelatie/00000000-0000-0000-4000-000000000000-b25kZXJkZWVsI0hlZWZ0QmV0cm9ra2VuZQ",
            "RelatieObject.bron": {
                "@type": "http://purl.org/dc/terms/Agent",
                "@id": "https://data.awvvlaanderen.be/id/asset/10000000-0000-0000-0000-000000000000-b25kZXJkZWVsI05ldHdlcmtrYWFydA"
            },
            "RelatieObject.doel": {
                "@type": "http://purl.org/dc/terms/Agent",
                "@id": "https://data.awvvlaanderen.be/id/asset/20000000-0000-0000-0000-000000000000-cHVybDpBZ2VudA"
            },
            "HeeftBetrokkene.rol": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlBetrokkenheidRol/lid",
            "HeeftBetrokkene.specifiekeContactinfo": [],
            "AIMDBStatus.isActief": True
        }]
