import logging
import time
from datetime import datetime, timezone

from requests.exceptions import ConnectionError

from AgentFeedEventsCollector import AgentFeedEventsCollector
from AgentFeedEventsProcessor import AgentFeedEventsProcessor
from AgentUpdater import AgentUpdater
from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from ResourceEnum import ResourceEnum, colorama_table
from SyncTimer import SyncTimer


class AgentSyncer:
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter):
        self.postgis_connector: PostGISConnector = postgis_connector
        self.eminfra_importer: EMInfraImporter = eminfra_importer
        self.updater: AgentUpdater = AgentUpdater()
        self.events_collector: AgentFeedEventsCollector = AgentFeedEventsCollector(eminfra_importer)
        self.events_processor: AgentFeedEventsProcessor = AgentFeedEventsProcessor(postgis_connector,
                                                                                   eminfra_importer=eminfra_importer)
        self.color = colorama_table[ResourceEnum.agents]

    def sync(self, connection, stop_when_fully_synced: bool=False):
        while True:
            try:
                sync_allowed_by_time = SyncTimer.calculate_sync_allowed_by_time()
                if not sync_allowed_by_time:
                    logging.info(f'{self.color}syncing is not allowed at this time. Trying again in 5 minutes')
                    time.sleep(300)
                    continue

                params = self.postgis_connector.get_params(connection)
                current_page = params['page_agents']
                completed_event_id = params['event_uuid_agents']
                page_size = params['pagesize']

                logging.info(f'{self.color}starting a sync cycle for agents, page: {str(current_page)} '
                             f'event_uuid: {str(completed_event_id)}')
                start = time.time()

                try:
                    eventsparams_to_process = self.events_collector.collect_starting_from_page(
                        current_page, completed_event_id, page_size, resource='agents')

                    total_events = sum(len(lists) for lists in eventsparams_to_process.event_dict.values())
                    if total_events == 0:
                        logging.info(f"{self.color}The database is fully synced for agents. Continuing keep up to date in 30 seconds")
                        self.postgis_connector.update_params(params={'last_update_utc_agents': datetime.now(timezone.utc)},
                                                             connection=connection)
                        if stop_when_fully_synced:
                            break
                        time.sleep(30)  # wait 30 seconds to prevent overloading API
                        continue
                    end = time.time()
                    self.log_eventparams(eventsparams_to_process.event_dict, round(end - start, 2), color=self.color)
                except ConnectionError:
                    logging.info(f"{self.color}failed connection, retrying in 1 minute")
                    connection.rollback()
                    time.sleep(60)
                    continue
                except Exception as exc:
                    logging.error(f'{self.color}{exc}')
                    connection.rollback()
                    time.sleep(30)
                    continue

                try:
                    self.events_processor.process_events(eventsparams_to_process, connection)
                except Exception as exc:
                    logging.error(f'{self.color}{exc}')
                    connection.rollback()
                    time.sleep(30)

                sync_allowed_by_time = SyncTimer.calculate_sync_allowed_by_time()
            except ConnectionError:
                logging.info(f"{self.color}failed connection, retrying in 1 minute")
                time.sleep(60)
            except Exception as exc:
                logging.error(f'{self.color}{exc}')
                time.sleep(30)

    @staticmethod
    def log_eventparams(event_dict, timespan: float, color):
        total = sum(len(events) for events in event_dict.values())
        logging.info(f'{color}fetched {total} agents events to sync in {timespan} seconds')
        for k, v in event_dict.items():
            if len(v) > 0:
                logging.info(f'{color}number of events of type {k}: {len(v)}')
