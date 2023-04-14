import logging
import time
import traceback
from datetime import datetime

from BetrokkeneRelatieFeedEventsCollector import BetrokkeneRelatieFeedEventsCollector
from BetrokkeneRelatieFeedEventsProcessor import BetrokkeneRelatieFeedEventsProcessor
from BetrokkeneRelatiesUpdater import BetrokkeneRelatiesUpdater
from EMInfraImporter import EMInfraImporter
from Exceptions.AgentMissingError import AgentMissingError
from Exceptions.AssetMissingError import AssetMissingError
from PostGISConnector import PostGISConnector
from ResourceEnum import colorama_table, ResourceEnum
from SyncTimer import SyncTimer


class BetrokkeneRelatieSyncer:
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter):
        self.postgis_connector: PostGISConnector = postgis_connector
        self.eminfra_importer: EMInfraImporter = eminfra_importer
        self.updater: BetrokkeneRelatiesUpdater = BetrokkeneRelatiesUpdater()
        self.events_collector: BetrokkeneRelatieFeedEventsCollector = BetrokkeneRelatieFeedEventsCollector(
            eminfra_importer=eminfra_importer)
        self.events_processor: BetrokkeneRelatieFeedEventsProcessor = BetrokkeneRelatieFeedEventsProcessor(
            postgis_connector, eminfra_importer=eminfra_importer)
        self.color = colorama_table[ResourceEnum.betrokkenerelaties]

    def sync(self, connection):
        while True:
            try:
                sync_allowed_by_time = SyncTimer.calculate_sync_allowed_by_time()
                if not sync_allowed_by_time:
                    logging.info(self.color + 'syncing is not allowed at this time. Trying again in 5 minutes')
                    time.sleep(300)
                    continue

                params = self.postgis_connector.get_params(connection)
                current_page = params['page_betrokkenerelaties']
                completed_event_id = params['event_uuid_betrokkenerelaties']
                page_size = params['pagesize']

                logging.info(self.color + f'starting a sync cycle for betrokkenerelaties, page: {str(current_page)} event_uuid: {str(completed_event_id)}')
                start = time.time()

                eventsparams_to_process = None
                try:
                    eventsparams_to_process = self.events_collector.collect_starting_from_page(
                        current_page, completed_event_id, page_size, resource='betrokkenerelaties')

                    total_events = sum(len(lists) for lists in eventsparams_to_process.event_dict.values())
                    if total_events == 0:
                        logging.info(self.color + 'The database is fully synced for betrokkenerelaties. Continuing keep up to date in 30 seconds')
                        self.postgis_connector.update_params(params={'last_update_utc_betrokkenerelaties': datetime.utcnow()},
                                                             connection=connection)
                        time.sleep(30)  # wait 30 seconds to prevent overloading API
                        continue
                except ConnectionError as err:
                    print(err)
                    logging.info(self.color + "failed connection, retrying in 1 minute")
                    time.sleep(60)
                    continue
                except Exception as err:
                    print(err)
                    end = time.time()
                    self.log_eventparams(eventsparams_to_process.event_dict, round(end - start, 2), self.color)
                    time.sleep(30)
                    continue

                try:
                    self.events_processor.process_events(eventsparams_to_process, connection)
                except AssetMissingError or AgentMissingError:
                    logging.warning(self.color + 'Tried to add betrokkenerelaties but a source or target is missing. '
                                    'Trying again in 60 seconds to allow other feeds to create the missing objects.')
                    time.sleep(60)
                    continue
                except Exception as exc:
                    traceback.print_exception(exc)
                    connection.rollback()
                    time.sleep(30)
            except ConnectionError as err:
                print(err)
                logging.info(self.color + "failed connection, retrying in 1 minute")
                time.sleep(60)
            except Exception as err:
                logging.error(self.color + err)
                time.sleep(30)

            sync_allowed_by_time = SyncTimer.calculate_sync_allowed_by_time()

    @staticmethod
    def log_eventparams(event_dict, timespan: float, color):
        total = sum(len(events) for events in event_dict.values())
        logging.info(color + f'fetched {total} betrokkenerelaties events to sync in {timespan} seconds')
        for k, v in event_dict.items():
            if len(v) > 0:
                logging.info(color + f'number of events of type {k}: {len(v)}')