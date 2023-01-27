import logging
import time
import traceback
from datetime import datetime

from AssetRelatieFeedEventsCollector import AssetRelatieFeedEventsCollector
from AssetRelatieFeedEventsProcessor import AssetRelatieFeedEventsProcessor
from AssetRelatiesUpdater import AssetRelatiesUpdater
from EMInfraImporter import EMInfraImporter
from Exceptions.AssetMissingError import AssetMissingError
from PostGISConnector import PostGISConnector
from SyncTimer import SyncTimer


class AssetRelatieSyncer:
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter):
        self.postgis_connector: PostGISConnector = postgis_connector
        self.eminfra_importer: EMInfraImporter = eminfra_importer
        self.updater: AssetRelatiesUpdater = AssetRelatiesUpdater()
        self.events_collector: AssetRelatieFeedEventsCollector = AssetRelatieFeedEventsCollector(
            eminfra_importer=eminfra_importer)
        self.events_processor: AssetRelatieFeedEventsProcessor = AssetRelatieFeedEventsProcessor(
            postgis_connector, eminfra_importer=eminfra_importer)

    def sync(self, connection):
        sync_allowed_by_time = SyncTimer.calculate_sync_allowed_by_time()

        while sync_allowed_by_time:
            params = self.postgis_connector.get_params(connection)
            current_page = params['page_assetrelaties']
            completed_event_id = params['event_uuid_assetrelaties']
            page_size = params['pagesize']

            logging.info(f'starting a sync cycle for assetrelaties, page: {str(current_page + 1)} event_uuid: {str(completed_event_id)}')
            start = time.time()

            eventsparams_to_process = None
            try:
                eventsparams_to_process = self.events_collector.collect_starting_from_page(
                    current_page, completed_event_id, page_size, resource='assetrelaties')

                total_events = sum(len(lists) for lists in eventsparams_to_process.event_dict.values())
                if total_events == 0:
                    logging.info(f"The database is fully synced for assetrelaties. Continuing keep up to date in 30 seconds")
                    self.postgis_connector.update_params(params={'last_update_utc_assetrelaties': datetime.utcnow()},
                                                         connection=connection)
                    time.sleep(30)  # wait 30 seconds to prevent overloading API
                    continue
            except ConnectionError as err:
                print(err)
                logging.info("failed connection, retrying in 1 minute")
                time.sleep(60)
                continue
            except Exception as err:
                print(err)
                end = time.time()
                self.log_eventparams(eventsparams_to_process.event_dict, round(end - start, 2))
                time.sleep(10)
                continue

            try:
                self.events_processor.process_events(eventsparams_to_process, connection)
            except AssetMissingError:
                logging.warning(f"Tried to add assetrelaties but a source or target is missing. "
                                f"Trying again in 60 seconds to allow other feeds to create the missing objects.")
                time.sleep(60)
                continue
            except Exception as exc:
                traceback.print_exception(exc)
                connection.rollback()
                time.sleep(10)

            sync_allowed_by_time = SyncTimer.calculate_sync_allowed_by_time()

    @staticmethod
    def log_eventparams(event_dict, timespan: float):
        total = sum(len(events) for events in event_dict.values())
        logging.info(f'fetched {total} assetrelaties events to sync in {timespan} seconds')
        for k, v in event_dict.items():
            if len(v) > 0:
                logging.info(f'number of events of type {k}: {len(v)}')