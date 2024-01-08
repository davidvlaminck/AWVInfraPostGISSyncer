import concurrent
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from AgentSyncer import AgentSyncer
from AssetRelatieSyncer import AssetRelatieSyncer
from AssetSyncer import AssetSyncer
from BetrokkeneRelatieSyncer import BetrokkeneRelatieSyncer
from ControleficheSyncer import ControleficheSyncer
from EMInfraImporter import EMInfraImporter
from FeedEventsCollector import FeedEventsCollector
from FeedEventsProcessor import FeedEventsProcessor
from FillManager import FillManager
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from SyncTimer import SyncTimer


class SyncerFactory:
    @classmethod
    def get_syncer_by_feed_name(cls, feed, eminfra_importer: EMInfraImporter, postgis_connector: PostGISConnector):
        if feed == 'agents':
            time.sleep(1)
            return AgentSyncer(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector)
        elif feed == 'assets':
            time.sleep(2)
            return AssetSyncer(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector)
        elif feed == 'assetrelaties':
            time.sleep(3)
            return AssetRelatieSyncer(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector)
        elif feed == 'betrokkenerelaties':
            time.sleep(4)
            return BetrokkeneRelatieSyncer(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector)
        elif feed == 'controlefiches':
            time.sleep(5)
            return ControleficheSyncer(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector)


class SyncManager:
    def __init__(self, connector: PostGISConnector, request_handler: RequestHandler, eminfra_importer: EMInfraImporter,
                 settings=None):
        self.connector = connector
        self.request_handler = request_handler
        self.eminfra_importer = eminfra_importer
        self.events_collector = FeedEventsCollector(eminfra_importer)
        self.events_processor = FeedEventsProcessor(connector, eminfra_importer=eminfra_importer)
        self.settings = settings
        if 'time' in self.settings:
            SyncTimer.sync_start = self.settings['time']['start']
            SyncTimer.sync_end = self.settings['time']['end']

    def start(self):
        while True:
            try:
                params = self.connector.get_params(self.connector.main_connection)
                if params is None:
                    self.connector.set_up_tables()
                    params = self.connector.get_params(self.connector.main_connection)

                if params['fresh_start']:
                    filler = FillManager(connector=self.connector,
                                         eminfra_importer=self.eminfra_importer)
                    filler.fill(params)
                else:
                    self.perform_multiprocessing_syncing()
            except requests.exceptions.ConnectionError as exc:
                print(exc)
                time.sleep(10)
            except Exception as exc:
                print(exc)
                time.sleep(10)

    def start_sync_by_feed(self, feed):
        syncer = SyncerFactory.get_syncer_by_feed_name(feed, eminfra_importer=self.eminfra_importer,
                                                       postgis_connector=self.connector)
        connection = self.connector.get_connection()
        syncer.sync(connection=connection)

    def perform_multiprocessing_syncing(self):
        feeds = ['assets', 'agents', 'assetrelaties', 'betrokkenerelaties', 'controlefiches']

        # use multithreading
        executor = ThreadPoolExecutor(5)
        futures = [executor.submit(self.start_sync_by_feed, feed=feed)
                   for feed in feeds]
        concurrent.futures.wait(futures)
