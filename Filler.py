import concurrent
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import urllib3

from AgentFiller import AgentFiller
from AssetFiller import AssetFiller
from AssetRelatieFiller import AssetRelatieFiller
from AssetTypeFiller import AssetTypeFiller
from BeheerderFiller import BeheerderFiller
from BestekFiller import BestekFiller
from BestekKoppelingSyncer import BestekKoppelingSyncer
from BetrokkeneRelatieFiller import BetrokkeneRelatieFiller
from EMInfraImporter import EMInfraImporter
from Exceptions.AssetTypeMissingError import AssetTypeMissingError
from FeedEventsCollector import FeedEventsCollector
from FeedEventsProcessor import FeedEventsProcessor
from IdentiteitFiller import IdentiteitFiller
from PostGISConnector import PostGISConnector
from RelatieTypeFiller import RelatieTypeFiller
from RequestHandler import RequestHandler
from ToezichtgroepFiller import ToezichtgroepFiller


class Filler:
    def __init__(self, connector: PostGISConnector, request_handler: RequestHandler,
                 eminfra_importer: EMInfraImporter, ):
        self.connector = connector
        self.request_handler = request_handler
        self.eminfra_importer = eminfra_importer
        self.events_collector = FeedEventsCollector(eminfra_importer)
        self.events_processor = FeedEventsProcessor(connector, eminfra_importer)

    def fill_table(self, table_to_fill, page_size, fill, cursor):
        if not fill:
            return
        if table_to_fill == 'agents':
            self.fill_agents(page_size, cursor)
        elif table_to_fill == 'toezichtgroepen':
            self.fill_toezichtgroepen(page_size, cursor)
        elif table_to_fill == 'beheerders':
            self.fill_beheerders(page_size, cursor)
        elif table_to_fill == 'betrokkenerelaties':
            self.fill_betrokkenerelaties(page_size, cursor)
        elif table_to_fill == 'assetrelaties':
            self.fill_assetrelaties(page_size, cursor)
        elif table_to_fill == 'bestekken':
            self.fill_bestekken(page_size, cursor)
        elif table_to_fill == 'assettypes':
            self.fill_assettypes(page_size, cursor)
        elif table_to_fill == 'toezichtgroepen':
            self.fill_toezichtgroepen(page_size, cursor)
        elif table_to_fill == 'identiteiten':
            self.fill_identiteiten(page_size, cursor)
        elif table_to_fill == 'relatietypes':
            self.fill_relatietypes(page_size, cursor)
        elif table_to_fill == 'assets':
            self.fill_assets(page_size, cursor)
        elif table_to_fill == 'bestekkoppelingen':
            self.fill_bestekkoppelingen(page_size, cursor)

    def fill(self, params: dict):
        logging.info('Filling the database with data')
        page_size = params['pagesize']
        if 'page_assets' not in params or 'page_assetrelaties' not in params or \
                'page_agents' not in params or 'page_betrokkenerelaties' not in params:
            logging.info('Getting the last pages for feeds')
            feeds = ['assets', 'agents', 'assetrelaties', 'betrokkenerelaties']

            # use multithreading
            executor = ThreadPoolExecutor()
            futures = [executor.submit(self.save_last_feedevent_to_params, feed=feed, page_size=page_size)
                       for feed in feeds]
            concurrent.futures.wait(futures)

        tables_to_fill = []
        while True:
            try:
                # tables_to_fill = ['agents', 'toezichtgroepen', 'beheerders'] # , ''
                tables_to_fill = ['agents', 'bestekken', 'toezichtgroepen', 'identiteiten', 'relatietypes',
                                  'assettypes', 'beheerders', 'betrokkenerelaties', 'assetrelaties', 'assets', 'bestekkoppelingen']

                params = self.connector.get_params(self.connector.main_connection)
                if 'assets_fill' not in params:
                    self.create_params_for_table_fill(tables_to_fill, self.connector.main_connection)
                    params = self.connector.get_params(self.connector.main_connection)

                tables_to_fill_filtered = []
                for table_to_fill in tables_to_fill:
                    if params[table_to_fill + '_fill']:
                        tables_to_fill_filtered.append(table_to_fill)

                # use multithreading
                logging.info(f'filling {len(tables_to_fill_filtered)} tables ...')
                with concurrent.futures.ThreadPoolExecutor(len(tables_to_fill_filtered)) as executor:
                    futures = [executor.submit(self.fill_table, table_to_fill=table_to_fill,
                                               fill=params[table_to_fill + '_fill'],
                                               cursor=params[table_to_fill + '_cursor'], page_size=params['pagesize'])
                               for table_to_fill in tables_to_fill_filtered]
                    concurrent.futures.wait(futures)

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
            except urllib3.exceptions.ConnectionError as exc:
                logging.error(exc)
                logging.info('failed connection, retrying in 1 minute')
                time.sleep(60)
            except Exception as err:
                self.connector.main_connection.rollback()
                raise err

        print('Done with filling')
        self.delete_params_for_table_fill(tables_to_fill, connection=self.connector.main_connection)
        self.connector.update_params(connection=self.connector.main_connection, params={'fresh_start': False})

    def delete_params_for_table_fill(self, tables_to_fill, connection):
        param_dict = {}
        for table_to_fill in tables_to_fill:
            param_dict[table_to_fill + '_fill'] = True
            param_dict[table_to_fill + '_cursor'] = True
        self.connector.delete_params(param_dict, connection)

    def create_params_for_table_fill(self, tables_to_fill, connection):
        param_dict = {}
        for table_to_fill in tables_to_fill:
            param_dict[table_to_fill + '_fill'] = True
            param_dict[table_to_fill + '_cursor'] = ''
        self.connector.create_params(param_dict, connection)

    def fill_assetrelaties(self, page_size, pagingcursor):
        logging.info(f'Filling assetrelaties table')
        start = time.time()
        assetrelatie_syncer = AssetRelatieFiller(
            eminfra_importer=self.eminfra_importer, postgis_connector=self.connector, resource='assetrelaties')
        connection = self.connector.get_connection()
        params = None
        while True:
            try:
                params = self.connector.get_params(connection)
                assetrelatie_syncer.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
            except ConnectionError as exc:
                logging.error(exc)
                logging.info('Connection error: trying again in 60 seconds...')
                time.sleep(60)
                continue
            except urllib3.exceptions.ConnectionError as exc:
                logging.error(exc)
                logging.info('Connection error: trying again in 60 seconds...')
                time.sleep(60)
                continue
            except Exception as exc:
                logging.error(exc)
                logging.info('Unknown error, escalating it.')
                raise exc

                # missing_assets = exc.args[0]
                # logging.info(f'Syncing {len(missing_assets)} assets first.')
                # self.connector.create_params(params={'assets_ad_hoc': ''}, connection=connection)
                # asset_syncer = AssetSyncer(eminfra_importer=self.eminfra_importer, postgis_connector=self.connector,
                #                            resource='assets')
                # asset_syncer.sync_ad_hoc(missing_assets, connection=connection)
                # self.connector.delete_params(params={'assets_ad_hoc': ''}, connection=connection)
                # continue

            if self.eminfra_importer.paging_cursors['assetrelaties_cursor'] == '':
                break

        self.connector.update_params(params={'assetrelaties_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'time for all assetrelaties: {round(end - start, 2)}')

    def fill_betrokkenerelaties(self, page_size, pagingcursor):
        logging.info(f'Filling betrokkenerelaties table')
        start = time.time()
        betrokkenerelatie_syncer = BetrokkeneRelatieFiller(
            eminfra_importer=self.eminfra_importer, postgis_connector=self.connector, resource='betrokkenerelaties')
        connection = self.connector.get_connection()
        params = None
        while True:
            try:
                params = self.connector.get_params(connection)
                betrokkenerelatie_syncer.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
            except ConnectionError as exc:
                logging.error(exc)
                logging.info('Connection error: trying again in 60 seconds...')
                time.sleep(60)
                continue
            except urllib3.exceptions.ConnectionError as exc:
                logging.error(exc)
                logging.info('Connection error: trying again in 60 seconds...')
                time.sleep(60)
                continue
            except Exception as exc:
                logging.error(exc)
                logging.info('Unknown error, escalating it.')
                raise exc

                # missing_assets = exc.args[0]
                # logging.info(f'Syncing {len(missing_assets)} assets first.')
                # self.connector.create_params(params={'assets_ad_hoc': ''}, connection=connection)
                # asset_syncer = AssetSyncer(eminfra_importer=self.eminfra_importer, postgis_connector=self.connector,
                #                            resource='assets')
                # asset_syncer.sync_ad_hoc(missing_assets, connection=connection)
                # self.connector.delete_params(params={'assets_ad_hoc': ''}, connection=connection)
                # continue

                # Updater : updates objects
                #   logic how to update/create/delete objects
                #   SQL query's
                # Syncers : converts uuids to generator to be processed by Updater
                #   contains logic on how to convert uuids to dicts
                # Fillers : creates a generator to loop over all objects, to be processed by Updater
                # Fillers inherit from FastFiller, so they all use the same mechanic fill()
                #   contains logic on how to get all dicts

            if self.eminfra_importer.paging_cursors['betrokkenerelaties_cursor'] == '':
                break

        self.connector.update_params(params={'betrokkenerelaties_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'time for all betrokkenerelaties: {round(end - start, 2)}')

    def fill_bestekkoppelingen(self, page_size, pagingcursor):
        logging.info(f'Filling bestekkoppelingen table')
        params = self.connector.get_params(self.connector.main_connection)
        if 'assets_fill' not in params:
            raise ValueError('missing assets_fill in params')
        if params['assets_fill']:
            logging.info(f'Waiting for assets to be filled')
            return

        start = time.time()
        bestek_koppeling_syncer = BestekKoppelingSyncer(em_infra_importer=self.eminfra_importer,
                                                        postGIS_connector=self.connector)
        bestek_koppeling_syncer.sync_bestekkoppelingen()
        end = time.time()
        logging.info(f'time for all bestekkoppelingen: {round(end - start, 2)}')

    def fill_assets(self, page_size, pagingcursor):
        logging.info(f'Filling assets table')
        start = time.time()
        asset_filler = AssetFiller(eminfra_importer=self.eminfra_importer, postgis_connector=self.connector,
                                   resource='assets')
        connection = self.connector.get_connection()
        while True:
            try:
                asset_filler.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
            except AssetTypeMissingError:
                params = self.connector.get_params(connection)
                if 'assettypes_fill' in params and params['assettypes_fill']:
                    logging.info('Assettype(s) missing while filling. Trying again in 60 seconds')
                    time.sleep(60)
                    continue
            except ConnectionError as exc:
                logging.error(exc)
                logging.info('Connection error: trying again in 60 seconds...')
                time.sleep(60)
                continue
            except urllib3.exceptions.ConnectionError as exc:
                logging.error(exc)
                logging.info('Connection error: trying again in 60 seconds...')
                time.sleep(60)
                continue
            except Exception as exc:
                logging.error(exc)
                logging.info('Unknown error, escalating it.')
                raise exc

            if self.eminfra_importer.paging_cursors['assets_cursor'] == '':
                break

        self.connector.update_params(params={'assets_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'Time for all assets: {round(end - start, 2)}')

        # start = time.time()
        # asset_syncer = AssetSyncer(em_infra_importer=self.eminfra_importer,
        #                            postgis_connector=self.connector)
        # params = None
        # while True:
        #     try:
        #         params = self.connector.get_params()
        #         asset_syncer.sync_assets(pagingcursor=params['pagingcursor'])
        #     except (AssetTypeMissingError, AttribuutMissingError):
        #         self.connector.connection.rollback()
        #         print('refreshing assettypes')
        #         current_paging_cursor = self.eminfra_importer.pagingcursor
        #         self.eminfra_importer.pagingcursor = ''
        #         self.fill_assettypes(page_size=params['pagesize'], pagingcursor='')
        #         self.eminfra_importer.pagingcursor = current_paging_cursor
        #         self.connector.save_props_to_params({'pagingcursor': current_paging_cursor})
        #     except (ToezichtgroepMissingError):
        #         self.connector.connection.rollback()
        #         print('refreshing toezichtgroepen')
        #         current_paging_cursor = self.eminfra_importer.pagingcursor
        #         self.eminfra_importer.pagingcursor = ''
        #         self.fill_toezichtgroepen(page_size=params['pagesize'], pagingcursor='')
        #         self.eminfra_importer.pagingcursor = current_paging_cursor
        #         self.connector.save_props_to_params({'pagingcursor': current_paging_cursor})
        #
        #     if self.eminfra_importer.pagingcursor == '':
        #         break
        #
        # end = time.time()
        # logging.info(f'time for all assets: {round(end - start, 2)}')

    def fill_relatietypes(self, page_size, pagingcursor):
        logging.info(f'Filling relatietypes table')
        start = time.time()
        relatietypes_filler = RelatieTypeFiller(eminfra_importer=self.eminfra_importer,
                                                postgis_connector=self.connector, resource='relatietypes')
        connection = self.connector.get_connection()
        relatietypes_filler.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
        self.connector.update_params(params={'relatietypes_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'time for all relatietypes: {round(end - start, 2)}')

    def fill_assettypes(self, page_size, pagingcursor):
        logging.info(f'Filling assettypes table')
        start = time.time()
        assettype_filler = AssetTypeFiller(eminfra_importer=self.eminfra_importer, postgis_connector=self.connector,
                                           resource='assettypes')
        connection = self.connector.get_connection()
        assettype_filler.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
        self.connector.update_params(params={'assettypes_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'time for all assettypes: {round(end - start, 2)}')

    def fill_bestekken(self, page_size, pagingcursor):
        logging.info(f'Filling bestekken table')
        start = time.time()
        bestek_filler = BestekFiller(eminfra_importer=self.eminfra_importer, postgis_connector=self.connector,
                                     resource='bestekken')
        connection = self.connector.get_connection()
        bestek_filler.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
        self.connector.update_params(params={'bestekken_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'Time for all bestekken: {round(end - start, 2)}')

    def fill_beheerders(self, page_size, pagingcursor):
        logging.info(f'Filling beheerders table')
        start = time.time()
        beheerder_filler = BeheerderFiller(eminfra_importer=self.eminfra_importer, postgis_connector=self.connector,
                                           resource='beheerders')
        connection = self.connector.get_connection()
        beheerder_filler.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
        self.connector.update_params(params={'beheerders_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'Time for all beheerders: {round(end - start, 2)}')

    def fill_identiteiten(self, page_size, pagingcursor):
        logging.info(f'Filling identiteiten table')
        start = time.time()
        identiteit_filler = IdentiteitFiller(eminfra_importer=self.eminfra_importer, postgis_connector=self.connector,
                                             resource='identiteiten')
        connection = self.connector.get_connection()
        identiteit_filler.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
        self.connector.update_params(params={'identiteiten_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'time for all identiteiten: {round(end - start, 2)}')

    def fill_toezichtgroepen(self, page_size, pagingcursor):
        logging.info(f'Filling toezichtgroepen table')
        start = time.time()
        toezichtgroep_filler = ToezichtgroepFiller(eminfra_importer=self.eminfra_importer,
                                                   postgis_connector=self.connector, resource='toezichtgroepen')
        connection = self.connector.get_connection()
        toezichtgroep_filler.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
        self.connector.update_params(params={'toezichtgroepen_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'Time for all toezichtgroepen: {round(end - start, 2)}')

    def fill_agents(self, page_size, pagingcursor):
        logging.info(f'Filling agents table')
        start = time.time()
        agent_filler = AgentFiller(eminfra_importer=self.eminfra_importer, postgis_connector=self.connector,
                                   resource='agents')
        connection = self.connector.get_connection()
        agent_filler.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
        self.connector.update_params(params={'agents_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'Time for all agents: {round(end - start, 2)}')

    def save_last_feedevent_to_params(self, page_size: int, feed: str):
        logging.info(f'Searching last page of current feed for {feed}')
        start_num = 1
        step = 5
        start_num = self.recur_exp_find_start_page(current_num=start_num, step=step, page_size=page_size, feed=feed)
        if start_num < step:
            start_num = step
        current_page_num = self.recur_find_last_page(current_num=int(start_num / step),
                                                     current_step=int(start_num / step),
                                                     step=step, page_size=page_size, feed=feed)

        # doublecheck
        event_page = self.eminfra_importer.get_events_from_proxyfeed(page_num=current_page_num, page_size=page_size,
                                                                     resource=feed)
        links = event_page['links']
        prev_link = next((l for l in links if l['rel'] == 'previous'), None)
        if prev_link is not None:
            raise RuntimeError('algorithm did not result in the last page')

        # find last event_id
        entries = event_page['entries']
        last_event_uuid = entries[0]['id']

        self.connector.create_params(
            {f'event_uuid_{feed}': last_event_uuid,
             f'page_{feed}': current_page_num,
             f'last_update_utc_{feed}': None},
            connection=self.connector.main_connection)
        logging.info(f'Added last page of current feed for {feed} to params (page: {current_page_num})')

    def recur_exp_find_start_page(self, current_num, step, page_size, feed):
        event_page = None
        try:
            event_page = self.eminfra_importer.get_events_from_proxyfeed(page_num=current_num, page_size=page_size,
                                                                         resource=feed)
        except Exception as ex:
            if ex.args[0] == 'status 400':
                return current_num
        if event_page is None or 'message' not in event_page:
            return self.recur_exp_find_start_page(current_num=current_num * step, step=step, page_size=page_size,
                                                  feed=feed)
        return current_num

    def recur_find_last_page(self, current_num, current_step, step, page_size, feed):
        new_i = 0
        for i in range(step + 1):
            new_num = current_num + current_step * i
            try:
                self.eminfra_importer.get_events_from_proxyfeed(page_num=new_num, page_size=page_size, resource=feed)
            except Exception as ex:
                if ex.args[0] == 'status 400':
                    new_i = i - 1
                    break

        if current_step == 1:
            return current_num + current_step * new_i

        return self.recur_find_last_page(current_num=current_num + current_step * new_i, step=step,
                                         current_step=int(current_step / step), page_size=page_size, feed=feed)

    @staticmethod
    def log_eventparams(event_dict, timespan: float):
        total = sum(len(events) for events in event_dict.values())
        logging.info(f'fetched {total} asset events to sync in {timespan} seconds')
        for k, v in event_dict.items():
            if len(v) > 0:
                logging.info(f'number of events of type {k}: {len(v)}')
