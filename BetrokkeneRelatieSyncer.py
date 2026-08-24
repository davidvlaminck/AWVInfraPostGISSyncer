import logging
import time
import traceback

from requests.exceptions import ConnectionError

from BetrokkeneRelatieFeedEventsCollector import BetrokkeneRelatieFeedEventsCollector
from BetrokkeneRelatieFeedEventsProcessor import BetrokkeneRelatieFeedEventsProcessor
from BetrokkeneRelatiesUpdater import BetrokkeneRelatiesUpdater
from EMInfraImporter import EMInfraImporter
from Exceptions.AgentMissingError import AgentMissingError
from Exceptions.AssetMissingError import AssetMissingError
from Helpers import is_pipeline_paused, now_in_brussels
from PostGISConnector import PostGISConnector
from ResourceEnum import colorama_table, ResourceEnum
from SyncTimer import SyncTimer


class BetrokkeneRelatieSyncer:
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter,
                 pipeline_state_db_path: str = None):
        self.postgis_connector: PostGISConnector = postgis_connector
        self.eminfra_importer: EMInfraImporter = eminfra_importer
        self.updater: BetrokkeneRelatiesUpdater = BetrokkeneRelatiesUpdater()
        self.events_collector: BetrokkeneRelatieFeedEventsCollector = BetrokkeneRelatieFeedEventsCollector(
            eminfra_importer=eminfra_importer)
        self.events_processor: BetrokkeneRelatieFeedEventsProcessor = BetrokkeneRelatieFeedEventsProcessor(
            postgis_connector, eminfra_importer=eminfra_importer)
        self.color = colorama_table[ResourceEnum.betrokkenerelaties]
        self.pipeline_state_db_path: str = pipeline_state_db_path

    def sync(self, connection, stop_when_fully_synced: bool = False):
        while True:
            try:
                if is_pipeline_paused(self.pipeline_state_db_path):
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                    time.sleep(60)
                    continue

                in_pause_window = SyncTimer.calculate_sync_paused_by_time()
                if in_pause_window:
                    logging.info(
                        self.color + 'in pause window (03:00-07:30), waiting for pipeline pause signal...'
                    )
                    try:
                        connection.rollback()
                    except Exception:
                        pass
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

                    if eventsparams_to_process is None:
                        logging.error(f"{self.color}collect_starting_from_page returned None, retrying in 30 seconds")
                        time.sleep(30)
                        continue

                    total_events = sum(len(lists) for lists in eventsparams_to_process.event_dict.values())
                    if total_events == 0:
                        logging.info(self.color + 'The database is fully synced for betrokkenerelaties. Continuing keep up to date in 30 seconds')
                        self.postgis_connector.update_params(params={'last_update_utc_betrokkenerelaties': now_in_brussels()},
                                                             connection=connection)
                        if stop_when_fully_synced:
                            break
                        time.sleep(30)  # wait 30 seconds to prevent overloading API
                        continue
                except ConnectionError:
                    logging.info(self.color + "failed connection, retrying in 1 minute")
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                    time.sleep(60)
                    continue
                except Exception as err:
                    logging.error(err)
                    end = time.time()
                    if eventsparams_to_process is not None:
                        self.log_eventparams(eventsparams_to_process.event_dict, round(end - start, 2), self.color)
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                    time.sleep(30)
                    continue

                try:
                    self.events_processor.process_events(eventsparams_to_process, connection)
                except ConnectionError:
                    logging.info(self.color + "failed connection, retrying in 1 minute")
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                    time.sleep(60)
                    continue
                except (AssetMissingError, AgentMissingError):
                    logging.warning(self.color + 'Tried to add betrokkenerelaties but a source or target is missing. '
                                    'Trying again in 60 seconds to allow other feeds to create the missing objects.')
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                    time.sleep(60)
                    continue
                except Exception as exc:
                    logging.error(exc)
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                    time.sleep(30)
            except ConnectionError:
                logging.info(self.color + "failed connection, retrying in 1 minute")
                try:
                    connection.rollback()
                except Exception:
                    pass
                time.sleep(60)
            except Exception as err:
                logging.error(self.color + err)
                try:
                    connection.rollback()
                except Exception:
                    pass
                time.sleep(30)

    @staticmethod
    def log_eventparams(event_dict, timespan: float, color):
        total = sum(len(events) for events in event_dict.values())
        logging.info(color + f'fetched {total} betrokkenerelaties events to sync in {timespan} seconds')
        for k, v in event_dict.items():
            if len(v) > 0:
                logging.info(color + f'number of events of type {k}: {len(v)}')