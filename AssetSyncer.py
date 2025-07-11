import logging
import time
from datetime import datetime, timezone

from requests.exceptions import ConnectionError

from AssetFeedEventsCollector import AssetFeedEventsCollector
from AssetFeedEventsProcessor import AssetFeedEventsProcessor
from AssetUpdater import AssetUpdater
from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.NieuwAssetProcessor import NieuwAssetProcessor
from Exceptions.AssetMissingError import AssetMissingError
from Exceptions.AssetTypeMissingError import AssetTypeMissingError
from Exceptions.AttribuutMissingError import AttribuutMissingError
from Exceptions.BeheerderMissingError import BeheerderMissingError
from Exceptions.BestekMissingError import BestekMissingError
from Exceptions.IdentiteitMissingError import IdentiteitMissingError
from Exceptions.ToezichtgroepMissingError import ToezichtgroepMissingError
from FillManager import FillManager
from PostGISConnector import PostGISConnector
from ResourceEnum import ResourceEnum, colorama_table
from SyncTimer import SyncTimer


class AssetSyncer:
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter):
        self.postgis_connector: PostGISConnector = postgis_connector
        self.eminfra_importer: EMInfraImporter = eminfra_importer
        self.updater: AssetUpdater = AssetUpdater()
        self.events_collector: AssetFeedEventsCollector = AssetFeedEventsCollector(eminfra_importer)
        self.events_processor: AssetFeedEventsProcessor = AssetFeedEventsProcessor(
            postgis_connector=postgis_connector, eminfra_importer=eminfra_importer)
        self.color = colorama_table[ResourceEnum.assets]

    def sync(self, connection, stop_when_fully_synced: bool = False):
        while True:
            try:
                sync_allowed_by_time = SyncTimer.calculate_sync_allowed_by_time()
                if not sync_allowed_by_time:
                    self.update_view_tables(connection, color=self.color)
                    logging.info(
                        f'{self.color}syncing is not allowed at this time. Trying again in 5 minutes'
                    )
                    time.sleep(300)
                    continue
                params = self.postgis_connector.get_params(connection)
                completed_event_id = params['event_uuid_assets']
                page_size = params['pagesize']

                current_page = params['page_assets']
                logging.info(
                    f'{self.color}starting a sync cycle for assets, page: {str(current_page)} event_uuid: {str(completed_event_id)}'
                )
                start = time.time()

                eventsparams_to_process = None
                try:
                    eventsparams_to_process = self.events_collector.collect_starting_from_page(
                        current_page, completed_event_id, page_size, resource='assets')

                    total_events = sum(len(lists) for lists in eventsparams_to_process.event_dict.values())
                    if total_events == 0:
                        logging.info(
                            f"{self.color}The database is fully synced for assets. Continuing keep up to date in 30 seconds"
                        )
                        self.postgis_connector.update_params(params={'last_update_utc_assets': datetime.now(timezone.utc)},
                                                             connection=connection)
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
                except BeheerderMissingError:
                    connection.rollback()
                    self.fill_resource(ResourceEnum.beheerders)
                except ToezichtgroepMissingError:
                    connection.rollback()
                    self.fill_resource(ResourceEnum.toezichtgroepen)
                except IdentiteitMissingError:
                    connection.rollback()
                    self.fill_resource(ResourceEnum.identiteiten)
                except BestekMissingError:
                    connection.rollback()
                    self.fill_resource(ResourceEnum.bestekken)
                except AssetMissingError as exc:
                    connection.rollback()
                    processor = NieuwAssetProcessor(eminfra_importer=self.eminfra_importer)
                    processor.process(uuids=exc.asset_uuids, connection=connection)
                    connection.commit()
                    continue
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
        self.eminfra_importer.paging_cursors['assets_ad_hoc'] = ''

        object_generator = self.eminfra_importer.import_resource_from_webservice_by_uuids(
            resource='assets', uuids=uuids)

        self.updater.update_objects(object_generator=object_generator, connection=connection,
                                    eminfra_importer=self.eminfra_importer)

    @staticmethod
    def log_eventparams(event_dict, timespan: float, color):
        total = sum(len(events) for events in event_dict.values())
        logging.info(
            f'{color}fetched {total} assets events to sync in {timespan} seconds'
        )
        for k, v in event_dict.items():
            if len(v) > 0:
                logging.info(f'{color}number of events of type {k}: {len(v)}')

    def update_view_tables(self, connection, color):
        try:
            params = self.postgis_connector.get_params(connection)
            last_update_views_date = params['last_update_utc_views'].date()
            today_date = (datetime.now(timezone.utc)).date()

            if today_date <= last_update_views_date:
                return

            select_view_names_query = "select viewname from pg_catalog.pg_views where schemaname = 'asset_views'"
            with connection.cursor() as cursor:
                cursor.execute(select_view_names_query)

                for view_name in cursor.fetchall():
                    view_name = view_name[0]
                    logging.info(f'{color}creating fixed table for {view_name}')
                    view_query = f"DROP TABLE IF EXISTS asset_daily_views.{view_name}; " \
                                 f"CREATE TABLE asset_daily_views.{view_name} AS SELECT * FROM asset_views.{view_name};"
                    cursor.execute(view_query)
                    connection.commit()

            self.postgis_connector.update_params(params={'last_update_utc_views': datetime.now(timezone.utc)},
                                                 connection=connection)
        except Exception as exc:
            logging.error(f"{self.color}Could not create view tables")
            logging.error(exc)
            connection.rollback()

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
