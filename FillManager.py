import concurrent
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import urllib3
from urllib3.exceptions import NewConnectionError

from BestekKoppelingSyncer import BestekKoppelingSyncer
from EMInfraImporter import EMInfraImporter
from Exceptions.FillResetError import FillResetError
from FeedEventsCollector import FeedEventsCollector
from FeedEventsProcessor import FeedEventsProcessor
from FillerFactory import FillerFactory
from PostGISConnector import PostGISConnector
from ResourceEnum import ResourceEnum, colorama_table


class FillManager:
    def __init__(self, connector: PostGISConnector, eminfra_importer: EMInfraImporter):
        self.connector = connector
        self.eminfra_importer = eminfra_importer
        self.events_collector = FeedEventsCollector(eminfra_importer)
        self.events_processor = FeedEventsProcessor(connector, eminfra_importer)
        self.reset_called: bool = False

    def fill_table(self, table_to_fill: ResourceEnum, page_size, fill, cursor):
        if not fill:
            return

        if table_to_fill == ResourceEnum.bestekkoppelingen.name:
            self.fill_bestekkoppelingen(page_size, cursor)
        else:
            self.fill_resource(page_size=page_size, pagingcursor=cursor, resource=table_to_fill)

    def fill(self, params: dict):
        logging.info('Filling the database with data')
        page_size = params['pagesize']
        if 'page_assets' not in params or 'page_assetrelaties' not in params or 'page_controlefiches' not in params or \
                'page_agents' not in params or 'page_betrokkenerelaties' not in params:
            logging.info('Getting the last pages for feeds')
            feeds = [ResourceEnum.assets.name, ResourceEnum.agents.name, ResourceEnum.assetrelaties.name,
                     ResourceEnum.betrokkenerelaties.name, ResourceEnum.controlefiches.name]

            # use multithreading
            executor = ThreadPoolExecutor()
            futures = [executor.submit(self.save_last_feedevent_to_params, feed=feed, page_size=page_size)
                       for feed in feeds]
            concurrent.futures.wait(futures)

        if 'last_update_utc_views' not in params:
            self.connector.create_params(params={'last_update_utc_views': '2023-01-01 00:00:00.000+01'},
                                         connection=self.connector.main_connection)

        while True:
            try:
                tables_to_fill = ResourceEnum.__members__

                params = self.connector.get_params(self.connector.main_connection)
                if 'assets_fill' not in params:
                    self.create_params_for_table_fill(tables_to_fill, self.connector.main_connection)
                    params = self.connector.get_params(self.connector.main_connection)

                tables_to_fill_filtered = [table_to_fill for table_to_fill in tables_to_fill
                                           if params[table_to_fill + '_fill']]

                # use multithreading
                logging.info(f'filling {len(tables_to_fill_filtered)} tables ...')
                with concurrent.futures.ThreadPoolExecutor(len(tables_to_fill_filtered)) as executor:
                    futures = [executor.submit(self.fill_table, table_to_fill=table_to_fill,
                                               fill=params[table_to_fill + '_fill'],
                                               cursor=params[table_to_fill + '_cursor'], page_size=params['pagesize'])
                               for table_to_fill in tables_to_fill_filtered]
                    concurrent.futures.wait(futures)

                self.reset_called = False

                params = self.connector.get_params(self.connector.main_connection)
                done = True
                for table_to_fill in tables_to_fill:
                    if params[table_to_fill + '_fill']:
                        done = False
                if done:
                    break

            except ConnectionError as err:
                print(err)
                logging.info("failed connection, retrying in 1 minute")
                time.sleep(60)
            except NewConnectionError as exc:
                logging.error(exc)
                logging.info('failed connection, retrying in 1 minute')
                time.sleep(60)
            except Exception as err:
                self.connector.main_connection.rollback()
                raise err

        print('Done with filling')
        self.delete_params_for_table_fill(tables_to_fill, connection=self.connector.main_connection)
        self.connector.update_params(connection=self.connector.main_connection, params={'fresh_start': False})

    def delete_params_for_table_fill(self, tables_to_fill: [ResourceEnum], connection):
        param_dict = {}
        for table_to_fill in tables_to_fill:
            param_dict[table_to_fill + '_fill'] = True
            param_dict[table_to_fill + '_cursor'] = True
        self.connector.delete_params(param_dict, connection)

    def create_params_for_table_fill(self, tables_to_fill: [ResourceEnum], connection):
        param_dict = {}
        for table_to_fill in tables_to_fill:
            param_dict[table_to_fill + '_fill'] = True
            param_dict[table_to_fill + '_cursor'] = ''
        self.connector.create_params(param_dict, connection)

    def fill_bestekkoppelingen(self, page_size, pagingcursor):
        # TODO write own loop to check for assets_fill
        # else: use fill_resource
        logging.info(colorama_table[ResourceEnum.bestekkoppelingen] + 'Filling bestekkoppelingen table')
        params = self.connector.get_params(self.connector.main_connection)
        if 'assets_fill' not in params:
            raise ValueError('missing assets_fill in params')
        if params['assets_fill']:
            logging.info(colorama_table[ResourceEnum.bestekkoppelingen] + 'Waiting for assets to be filled')
            return

        start = time.time()
        bestek_koppeling_syncer = BestekKoppelingSyncer(em_infra_importer=self.eminfra_importer,
                                                        postGIS_connector=self.connector)
        bestek_koppeling_syncer.sync_bestekkoppelingen()
        end = time.time()
        logging.info(colorama_table[ResourceEnum.bestekkoppelingen] + f'time for all bestekkoppelingen: {round(end - start, 2)}')

    def fill_resource(self, page_size, pagingcursor, resource: ResourceEnum):
        color = colorama_table[resource]
        logging.info(color + f'Filling {resource} table')
        connection = self.connector.get_connection()

        filler = FillerFactory.create_filler(eminfra_importer=self.eminfra_importer, resource=resource,
                                             postgis_connector=self.connector, fill_manager=self)
        while True:
            try:
                if filler.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection):
                    break
            except FillResetError:
                return
            except ConnectionError as exc:
                logging.error(exc)
                logging.info(color + 'Connection error: trying again in 60 seconds...')
                time.sleep(60)
                continue
            except urllib3.exceptions.ConnectionError as exc:
                logging.error(exc)
                logging.info(color + 'Connection error: trying again in 60 seconds...')
                time.sleep(60)
                continue
            except Exception as exc:
                logging.error(color + 'Unknown error. Hiding it!!')
                logging.error(exc)
                time.sleep(10)
                continue

        self.connector.kill_connection(connection)

    def save_last_feedevent_to_params(self, page_size: int, feed: str):
        logging.info(f'Getting last page of current feed for {feed}')

        while True:
            try:
                event_page = self.eminfra_importer.get_events_from_proxyfeed(page_num=-1, page_size=page_size,
                                                                             resource=feed)
                self_link = next(l for l in event_page['links'] if l['rel'] == 'self')
                current_page_num = int(self_link['href'][1:].split('/')[0])
                break
            except Exception as ex:
                logging.error(ex.args[0])
                time.sleep(60)

        # find last event_id
        entries = event_page['entries']
        last_event_uuid = entries[0]['id']

        self.connector.create_params(
            {f'event_uuid_{feed}': last_event_uuid,
             f'page_{feed}': current_page_num,
             f'last_update_utc_{feed}': None},
            connection=self.connector.main_connection)
        logging.info(f'Added last page of current feed for {feed} to params (page: {current_page_num})')
