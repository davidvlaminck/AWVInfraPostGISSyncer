import logging
import time
from datetime import datetime

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

    def sync(self, connection):
        sync_allowed_by_time = SyncTimer.calculate_sync_allowed_by_time()

        while sync_allowed_by_time:
            try:
                params = self.postgis_connector.get_params(connection)
                current_page = params['page_agents']
                completed_event_id = params['event_uuid_agents']
                page_size = params['pagesize']

                logging.info(self.color + f'starting a sync cycle for agents, page: {str(current_page)} event_uuid: {str(completed_event_id)}')
                start = time.time()

                try:
                    eventsparams_to_process = self.events_collector.collect_starting_from_page(
                        current_page, completed_event_id, page_size, resource='agents')

                    total_events = sum(len(lists) for lists in eventsparams_to_process.event_dict.values())
                    if total_events == 0:
                        logging.info(self.color + f"The database is fully synced for agents. Continuing keep up to date in 30 seconds")
                        self.postgis_connector.update_params(params={'last_update_utc_agents': datetime.utcnow()},
                                                             connection=connection)
                        time.sleep(30)  # wait 30 seconds to prevent overloading API
                        continue
                    end = time.time()
                    self.log_eventparams(eventsparams_to_process.event_dict, round(end - start, 2), color=self.color)
                except ConnectionError as err:
                    print(err)
                    logging.info(self.color + "failed connection, retrying in 1 minute")
                    connection.rollback()
                    time.sleep(60)
                    continue
                except Exception as err:
                    logging.error(self.color + err)
                    connection.rollback()
                    time.sleep(30)
                    continue

                try:
                    self.events_processor.process_events(eventsparams_to_process, connection)
                except Exception as exc:
                    logging.error(self.color + exc)
                    connection.rollback()
                    time.sleep(30)

                sync_allowed_by_time = SyncTimer.calculate_sync_allowed_by_time()
            except ConnectionError as err:
                print(err)
                logging.info(self.color + "failed connection, retrying in 1 minute")
                time.sleep(60)
            except Exception as err:
                logging.error(self.color + err)
                time.sleep(30)

    @staticmethod
    def log_eventparams(event_dict, timespan: float, color):
        total = sum(len(events) for events in event_dict.values())
        logging.info(color + f'fetched {total} agents events to sync in {timespan} seconds')
        for k, v in event_dict.items():
            if len(v) > 0:
                logging.info(color + f'number of events of type {k}: {len(v)}')
