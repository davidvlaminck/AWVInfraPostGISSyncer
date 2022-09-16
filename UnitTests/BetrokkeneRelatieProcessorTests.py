from unittest import TestCase
from unittest.mock import MagicMock

import psycopg2
from psycopg2 import connect

from EMInfraImporter import EMInfraImporter
from EventProcessors.BetrokkeneRelatiesGewijzigdProcessor import BetrokkeneRelatiesGewijzigdProcessor
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

        processor = BetrokkeneRelatiesGewijzigdProcessor(cursor=cursor, em_infra_importer=self.eminfra_importer, connector=self.connector)
        processor.em_infra_importer.import_betrokkenerelaties_from_webservice_by_assetuuids = self.return_betrokkenerelaties

        create_betrokkenerelatie_query = """
        INSERT INTO betrokkeneRelaties (uuid, agentUuid, assetUuid, rol, actief) 
        VALUES ('00000000-0000-0000-1000-000000000000', '10000000-0000-0000-0000-000000000000', 
        '00000000-0000-1000-0000-000000000000', 'toezichter', TRUE)"""
        select_asset_betrokkenerelatie_query = "SELECT assetUuid FROM betrokkeneRelaties WHERE uuid = '{uuid}'"
        select_agent_betrokkenerelatie_query = "SELECT agentUuid FROM betrokkeneRelaties WHERE uuid = '{uuid}'"
        count_betrokkenerelatie_query = "SELECT count(*) FROM betrokkeneRelaties"
        cursor = self.connector.connection.cursor()
        cursor.execute(create_betrokkenerelatie_query)

        with self.subTest('check the betrokkenerelatie created'):
            cursor.execute(select_asset_betrokkenerelatie_query.replace('{uuid}', '00000000-0000-0000-1000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('00000000-0000-1000-0000-000000000000', result)
            cursor.execute(select_agent_betrokkenerelatie_query.replace('{uuid}', '00000000-0000-0000-1000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('10000000-0000-0000-0000-000000000000', result)
        with self.subTest('number of betrokkenerelaties before update'):
            cursor.execute(count_betrokkenerelatie_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        processor.process(['00000000-0000-1000-0000-000000000000', '00000000-0000-2000-0000-000000000000',
                           '00000000-0000-3000-0000-000000000000'])

        with self.subTest('check after the betrokkenerelatie updated'):
            cursor.execute(select_asset_betrokkenerelatie_query.replace('{uuid}', '00000000-0000-0000-1000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('00000000-0000-1000-0000-000000000000', result)
            cursor.execute(select_agent_betrokkenerelatie_query.replace('{uuid}', '00000000-0000-0000-1000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('20000000-0000-0000-0000-000000000000', result)
        with self.subTest('check after the betrokkenerelatie created'):
            cursor.execute(select_asset_betrokkenerelatie_query.replace('{uuid}', '00000000-0000-0000-2000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('00000000-0000-2000-0000-000000000000', result)
        with self.subTest('number of betrokkenerelaties after update'):
            cursor.execute(count_betrokkenerelatie_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)

    def test_missing_agent(self):
        self.setup()

        cursor = self.connector.connection.cursor()
        self.set_up_one_agent(cursor)
        self.set_up_assets(cursor)

        self.connector.commit_transaction()

        processor = BetrokkeneRelatiesGewijzigdProcessor(cursor=cursor, em_infra_importer=self.eminfra_importer, connector=self.connector)
        processor.em_infra_importer.import_betrokkenerelaties_from_webservice_by_assetuuids = self.return_betrokkenerelaties

        with self.assertRaises(AgentMissingError) as exc:
            processor.process(['00000000-0000-1000-0000-000000000000', '00000000-0000-2000-0000-000000000000',
                               '00000000-0000-3000-0000-000000000000'])
        self.assertListEqual(['10000000-0000-0000-0000-000000000000'], exc.exception.args[0])

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
            "uuid": "00000000-0000-0000-1000-000000000000",
            "createdOn": "2021-01-01T00:00:00.000+01:00",
            "modifiedOn": "2021-01-01T00:00:00.000+01:00",
            "bron": {
                "_type": "onderdeel",
                "uuid": "00000000-0000-1000-0000-000000000000"
            },
            "doel": {
                "uuid": "20000000-0000-0000-0000-000000000000",
            },
            "rol": "toezichter"
        }, {
            "uuid": "00000000-0000-0000-2000-000000000000",
            "createdOn": "2021-01-01T00:00:00.000+01:00",
            "modifiedOn": "2021-01-01T00:00:00.000+01:00",
            "bron": {
                "_type": "onderdeel",
                "uuid": "00000000-0000-2000-0000-000000000000"
            },
            "doel": {
                "uuid": "10000000-0000-0000-0000-000000000000",
            },
            "rol": "toezichter"
        }, {
            "uuid": "00000000-0000-0000-3000-000000000000",
            "createdOn": "2021-01-01T00:00:00.000+01:00",
            "modifiedOn": "2021-01-01T00:00:00.000+01:00",
            "bron": {
                "_type": "onderdeel",
                "uuid": "00000000-0000-3000-0000-000000000000"
            },
            "doel": {
                "uuid": "20000000-0000-0000-0000-000000000000",
            },
            "rol": "toezichter"
        }]

