from unittest import TestCase
from unittest.mock import MagicMock

from psycopg2 import connect

from BestekSyncer import BestekSyncer
from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RelatietypeSyncer import RelatietypeSyncer
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager


class RelatietypeSyncerTests(TestCase):
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

    def test_sync_relatietypes(self):
        self.setup()

        cursor = self.connector.connection.cursor()
        cursor.execute("""
        INSERT INTO relatietypes (uuid, naam, actief, gericht) 
        VALUES ('10000000-0000-0000-0000-000000000000', 'test naam', TRUE, FALSE)""")

        select_relatietype_query = "SELECT naam FROM relatietypes WHERE uuid = '{uuid}'"
        count_relatietype_query = "SELECT count(*) FROM relatietypes"

        with self.subTest('naam check the first relatietype created'):
            cursor.execute(select_relatietype_query.replace('{uuid}', '10000000-0000-0000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('test naam', result)
        with self.subTest('number of relatietypes before update'):
            cursor.execute(count_relatietype_query)
            result = cursor.fetchone()[0]
            self.assertEqual(1, result)

        self.relatietype_syncer = RelatietypeSyncer(postgis_connector=self.connector,
                                                    em_infra_importer=self.eminfra_importer)
        self.relatietype_syncer.eminfra_importer.import_all_relatietypes_from_webservice = self.return_relatietypes

        self.relatietype_syncer.sync_relatietypes()

        with self.subTest('naam check after the first relatietype updated'):
            cursor.execute(select_relatietype_query.replace('{uuid}', '10000000-0000-0000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('HeeftAanvullendeGeometrie', result)
        with self.subTest('naam check after new relatietypes created'):
            cursor.execute(select_relatietype_query.replace('{uuid}', '20000000-0000-0000-0000-000000000000'))
            result = cursor.fetchone()[0]
            self.assertEqual('Omhult', result)
        with self.subTest('number of relatietypes after update'):
            cursor.execute(count_relatietype_query)
            result = cursor.fetchone()[0]
            self.assertEqual(3, result)

    @staticmethod
    def return_relatietypes():
        return [
            {
                "uuid": "10000000-0000-0000-0000-000000000000",
                "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftAanvullendeGeometrie",
                "naam": "HeeftAanvullendeGeometrie",
                "label": "Heeft aanvullende geometrie",
                "definitie": "Deze relatie legt een link tussen een object/onderdeel/installatie en een (bestands)bijlage waar een geometrie aan toegekend is. De richting loopt vanuit het fysiek object naar de bijlage met geometrie.",
                "gericht": True,
                "actief": True,
            }, {
                "uuid": "20000000-0000-0000-0000-000000000000",
                "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Omhult",
                "naam": "Omhult",
                "label": "Omhult",
                "definitie": "Deze relatie geeft aan dat het ene onderdeel het andere omhult. Dit vereist geen gesloten omhulling: de omhulling kan een open zijde hebben of bestaan uit open ruimtes. Deze relatie heeft een richting en gaat van het omhullende onderdeel naar het onderdeel dat omhuld wordt.",
                "gericht": True,
                "actief": True,
            }, {
                "uuid": "30000000-0000-0000-0000-000000000000",
                "uri": "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing",
                "naam": "Sturing",
                "label": "Sturing",
                "definitie": "Deze relatie geeft aan of er een of andere vorm van dataverkeer is tussen 2 onderdelen. Een wegverlichtingstoestel dat aan staat wordt ook als sturing beschouwd, in dit geval is het een lang ononderbroken elektrisch aan-signaal. Deze relatie heeft geen richting.",
                "gericht": False,
                "actief": True,
            }]
