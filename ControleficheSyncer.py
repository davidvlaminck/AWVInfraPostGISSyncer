import logging
import time
import traceback
from datetime import datetime

from requests.exceptions import ConnectionError

from ControleficheUpdater import ControleficheUpdater
from ControleficheFeedEventsCollector import ControleficheFeedEventsCollector
from ControleficheFeedEventsProcessor import ControleficheFeedEventsProcessor
from EMInfraImporter import EMInfraImporter
from Exceptions.AssetTypeMissingError import AssetTypeMissingError
from Exceptions.AttribuutMissingError import AttribuutMissingError
from FillManager import FillManager
from PostGISConnector import PostGISConnector
from ResourceEnum import ResourceEnum, colorama_table
from SyncTimer import SyncTimer


class ControleficheSyncer:
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter):
        self.postgis_connector: PostGISConnector = postgis_connector
        self.eminfra_importer: EMInfraImporter = eminfra_importer
        self.updater: ControleficheUpdater = ControleficheUpdater()
        self.events_collector: ControleficheFeedEventsCollector = ControleficheFeedEventsCollector(eminfra_importer)
        self.events_processor: ControleficheFeedEventsProcessor = ControleficheFeedEventsProcessor(
            postgis_connector=postgis_connector, eminfra_importer=eminfra_importer)
        self.color = colorama_table[ResourceEnum.controlefiches]

    def sync(self, connection, stop_when_fully_synced: bool = False):
        while True:
            try:
                sync_allowed_by_time = SyncTimer.calculate_sync_allowed_by_time()
                if not sync_allowed_by_time:
                    logging.info(f'{self.color}syncing is not allowed at this time. Trying again in 5 minutes')
                    time.sleep(300)
                    continue
                params = self.postgis_connector.get_params(connection)
                completed_event_id = params['event_uuid_controlefiches']
                page_size = params['pagesize']

                current_page = params['page_controlefiches']
                logging.info(
                    f'{self.color}starting a sync cycle for controlefiches, page: {str(current_page)} event_uuid: {str(completed_event_id)}'
                )
                start = time.time()

                eventsparams_to_process = None
                try:
                    eventsparams_to_process = self.events_collector.collect_starting_from_page(
                        current_page, completed_event_id, page_size, resource='controlefiches')

                    total_events = sum(len(lists) for lists in eventsparams_to_process.event_dict.values())
                    if total_events == 0:
                        logging.info(
                            f"{self.color}The database is fully synced for controlefiches. Continuing keep up to date in 30 seconds"
                        )
                        self.postgis_connector.update_params(
                            params={'last_update_utc_controlefiches': datetime.utcnow()}, connection=connection)
                        if stop_when_fully_synced:
                            break
                        time.sleep(30)  # wait 30 seconds to prevent overloading API
                        continue
                except ConnectionError:
                    logging.info(f"{self.color}failed connection, retrying in 1 minute")
                    time.sleep(60)
                    continue
                except Exception as err:
                    logging.error(err)
                    end = time.time()
                    self.log_eventparams(eventsparams_to_process.event_dict, round(end - start, 2), self.color)
                    time.sleep(30)
                    continue

                try:
                    self.events_processor.process_events(eventsparams_to_process, connection)
                except AssetTypeMissingError:
                    connection.rollback()
                    self.fill_resource(ResourceEnum.assettypes)
                except AttribuutMissingError:
                    connection.rollback()
                    self.fill_resource(ResourceEnum.assettypes)
                except ConnectionError:
                    logging.info(f"{self.color}failed connection, retrying in 1 minute")
                    time.sleep(60)
                except Exception as exc:
                    logging.error(exc)
                    connection.rollback()
                    time.sleep(30)
            except ConnectionError:
                logging.info(f"{self.color}failed connection, retrying in 1 minute")
                time.sleep(60)
            except Exception as exc:
                logging.error(f'{self.color}{exc}')
                time.sleep(30)

    def sync_by_uuids(self, uuids: [str], connection):
        self.eminfra_importer.paging_cursors['controlefiches_ad_hoc'] = ''

        object_generator = self.eminfra_importer.import_resource_from_webservice_by_uuids(
            resource='controlefiches', uuids=uuids)

        self.updater.update_objects(object_generator=object_generator, connection=connection,
                                    eminfra_importer=self.eminfra_importer)

    @staticmethod
    def log_eventparams(event_dict, timespan: float, color):
        total = sum(len(events) for events in event_dict.values())
        logging.info(
            f'{color}fetched {total} controlefiches events to sync in {timespan} seconds'
        )
        for k, v in event_dict.items():
            if len(v) > 0:
                logging.info(f'{color}number of events of type {k}: {len(v)}')

    def fill_resource(self, resource: ResourceEnum):
        fill_manager = FillManager(connector=self.postgis_connector,
                                   eminfra_importer=self.eminfra_importer)
        try:
            fill_manager.create_params_for_table_fill(resources_to_fill=[resource],
                                                      connection=self.postgis_connector.main_connection)
            fill_manager.fill_resource(100, pagingcursor='', resource=resource)
        except Exception as exc:
            logging.error(f"{self.color}Could not fill resource {resource}")
            logging.error(exc)
            self.postgis_connector.main_connection.rollback()
        finally:
            fill_manager.delete_params_for_table_fill(resources_to_fill=[resource],
                                                      connection=self.postgis_connector.main_connection)
