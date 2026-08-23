import concurrent
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import requests

from AgentSyncer import AgentSyncer
from AssetRelatieSyncer import AssetRelatieSyncer
from AssetSyncer import AssetSyncer
from BetrokkeneRelatieSyncer import BetrokkeneRelatieSyncer
from EMInfraImporter import EMInfraImporter
from FeedEventsCollector import FeedEventsCollector
from FeedEventsProcessor import FeedEventsProcessor
from FillManager import FillManager
from PauseManager import PauseManager
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from SyncTimer import SyncTimer


class SyncerFactory:
    @classmethod
    def get_syncer_by_feed_name(cls, feed, eminfra_importer: EMInfraImporter, postgis_connector: PostGISConnector,
                                pipeline_state_db_path: str = None):
        if feed == 'agents':
            time.sleep(1)
            return AgentSyncer(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                               pipeline_state_db_path=pipeline_state_db_path)
        elif feed == 'assets':
            time.sleep(2)
            return AssetSyncer(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                               pipeline_state_db_path=pipeline_state_db_path)
        elif feed == 'assetrelaties':
            time.sleep(3)
            return AssetRelatieSyncer(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                                      pipeline_state_db_path=pipeline_state_db_path)
        elif feed == 'betrokkenerelaties':
            time.sleep(4)
            return BetrokkeneRelatieSyncer(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                                           pipeline_state_db_path=pipeline_state_db_path)


class SyncManager:
    def __init__(self, connector: PostGISConnector, request_handler: RequestHandler, eminfra_importer: EMInfraImporter,
                 settings=None):
        self.connector = connector
        self.request_handler = request_handler
        self.eminfra_importer = eminfra_importer
        self.events_collector = FeedEventsCollector(eminfra_importer)
        self.events_processor = FeedEventsProcessor(connector, eminfra_importer=eminfra_importer)
        self.feeds = ['assets', 'agents', 'assetrelaties', 'betrokkenerelaties'] # removed controlefiches
        self.settings = settings
        if 'time' in self.settings:
            # The configured time window now represents when syncing is paused.
            SyncTimer.sync_start = self.settings['time']['start']
            SyncTimer.sync_end = self.settings['time']['end']
            SyncTimer.backup_time = self.settings['time'].get('backup', '06:00:00')

        self.pipeline_state_db_path = None
        if 'health_db' in self.settings:
            self.pipeline_state_db_path = self.settings['health_db'].get('path')

    def start(self, stop_when_fully_synced: bool = False):
        if self.pipeline_state_db_path and not getattr(self, '_pause_manager_started', False):
            self._pause_manager = PauseManager(
                db_path=self.pipeline_state_db_path,
                connector=self.connector,
                color='',
                backup_time=SyncTimer.backup_time,
            )
            self._pause_manager.start()
            self._pause_manager_started = True

        db_error_count = 0
        max_db_errors = 10

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
                    self.perform_multiprocessing_syncing(stop_when_fully_synced=stop_when_fully_synced)
                    if stop_when_fully_synced:
                        break
                db_error_count = 0
            except requests.exceptions.ConnectionError as exc:
                logging.error(exc)
                logging.info("failed connection, retrying in 30 seconds")
                self.connector.main_connection.rollback()
                time.sleep(30)
            except psycopg2.OperationalError as exc:
                db_error_count += 1
                logging.error(f"Database connection error ({db_error_count}/{max_db_errors}): {exc}")
                self.connector.main_connection.rollback()
                if db_error_count >= max_db_errors:
                    logging.critical("Too many consecutive database errors, exiting to allow restart")
                    os._exit(1)
                time.sleep(30)
            except Exception as exc:
                logging.error(exc)
                time.sleep(10)

    def start_sync_by_feed(self, feed, stop_when_fully_synced: bool = False):
        syncer = SyncerFactory.get_syncer_by_feed_name(feed, eminfra_importer=self.eminfra_importer,
                                                       postgis_connector=self.connector,
                                                       pipeline_state_db_path=self.pipeline_state_db_path)
        connection = self.connector.get_connection()
        try:
            syncer.sync(connection=connection, stop_when_fully_synced=stop_when_fully_synced)
        finally:
            self.connector.kill_connection(connection)

    def perform_multiprocessing_syncing(self, stop_when_fully_synced: bool):
        # use multithreading
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.start_sync_by_feed, feed=feed, stop_when_fully_synced=stop_when_fully_synced)
                       for feed in self.feeds]
            concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
