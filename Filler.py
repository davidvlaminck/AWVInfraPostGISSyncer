import concurrent
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor

from AgentFiller import AgentFiller
from AgentSyncer import AgentSyncer
from AssetRelatiesSyncer import AssetRelatiesSyncer
from AssetSyncer import AssetSyncer
from AssetTypeSyncer import AssetTypeSyncer
from BeheerderSyncer import BeheerderSyncer
from BestekKoppelingSyncer import BestekKoppelingSyncer
from BestekSyncer import BestekSyncer
from BetrokkeneRelatiesFiller import BetrokkeneRelatiesFiller
from BetrokkeneRelatiesSyncer import BetrokkeneRelatiesSyncer
from EMInfraImporter import EMInfraImporter
from EventProcessors.NieuwAssetProcessor import NieuwAssetProcessor
from Exceptions.AgentMissingError import AgentMissingError
from Exceptions.AssetMissingError import AssetMissingError
from Exceptions.AssetTypeMissingError import AssetTypeMissingError
from Exceptions.AttribuutMissingError import AttribuutMissingError
from Exceptions.ToezichtgroepMissingError import ToezichtgroepMissingError
from FeedEventsCollector import FeedEventsCollector
from FeedEventsProcessor import FeedEventsProcessor
from IdentiteitSyncer import IdentiteitSyncer
from PostGISConnector import PostGISConnector
from RelatietypeSyncer import RelatietypeSyncer
from RequestHandler import RequestHandler
from ToezichtgroepSyncer import ToezichtgroepSyncer


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
            self.sync_toezichtgroepen(page_size, cursor)
        elif table_to_fill == 'beheerders':
            self.sync_beheerders(page_size, cursor)
        elif table_to_fill == 'betrokkenerelaties':
            self.sync_betrokkenerelaties(page_size, cursor)

    def fill(self, params: dict):
        logging.info('Filling the database with data')
        page_size = params['pagesize']
        # TODO change to or when all feeds work
        # if 'page_assets' not in params or 'page_assetrelaties' not in params or \
        #         'page_agents' not in params or 'page_betrokkenerelaties' not in params:
        if 'page_assets' not in params:
            logging.info('Getting the last pages for feeds')
            feeds = ['assets', 'agents', 'assetrelaties', 'betrokkenerelaties']
            feeds = ['assets']

            # use multithreading
            executor = ThreadPoolExecutor()
            futures = [executor.submit(self.save_last_feedevent_to_params, feed=feed, page_size=page_size)
                       for feed in feeds]
            concurrent.futures.wait(futures)

        while True:
            try:
                # tables_to_fill = ['agents', 'toezichtgroepen', 'beheerders'] # , 'betrokkenerelaties'
                tables_to_fill = ['betrokkenerelaties']

                params = self.connector.get_params(self.connector.main_connection)
                if 'betrokkenerelaties_fill' not in params:
                    self.create_params_for_table_fill(tables_to_fill, self.connector.main_connection)
                    params = self.connector.get_params(self.connector.main_connection)

                # use multithreading
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(self.fill_table, table_to_fill=table_to_fill,
                                               fill=params[table_to_fill + '_fill'],
                                               cursor=params[table_to_fill + '_cursor'], page_size=params['pagesize'])
                               for table_to_fill in tables_to_fill]
                    concurrent.futures.wait(futures)

                params = self.connector.get_params(self.connector.main_connection)
                done = True
                for table_to_fill in tables_to_fill:
                    if params[table_to_fill + '_fill']:
                        done = False

                if done:
                    break
                else:
                    continue

                raise NotImplementedError()
                # main sync loop for a fresh start
                params = self.connector.get_params()
                sync_step = params['sync_step']
                pagingcursor = params['pagingcursor']
                page_size = params['pagesize']

                if sync_step == -1:
                    sync_step = 1
                if sync_step >= 12:
                    break

                if sync_step == 1:
                    self.fill(page_size, pagingcursor)
                elif sync_step == 2:
                    self.sync_toezichtgroepen(page_size, pagingcursor)
                elif sync_step == 3:
                    self.sync_identiteiten(page_size, pagingcursor)
                elif sync_step == 4:
                    self.sync_beheerders(page_size, pagingcursor)
                elif sync_step == 5:
                    self.sync_bestekken(page_size, pagingcursor)
                elif sync_step == 6:
                    self.sync_assettypes(page_size, pagingcursor)
                elif sync_step == 7:
                    self.sync_relatietypes()
                elif sync_step == 8:
                    self.sync_assets(page_size, pagingcursor)
                elif sync_step == 9:
                    self.sync_bestekkoppelingen()
                elif sync_step == 10:
                    self.sync_betrokkenerelaties()
                elif sync_step == 11:
                    self.sync_assetrelaties()
                else:
                    # TODO documenten
                    raise NotImplementedError

                pagingcursor = self.eminfra_importer.pagingcursor
                if pagingcursor == '':
                    sync_step += 1
                self.connector.save_props_to_params(
                    {'sync_step': sync_step,
                     'pagingcursor': pagingcursor})
                if sync_step >= 11:
                    self.connector.save_props_to_params(
                        {'fresh_start': False})
                self.connector.connection.commit()
            except ConnectionError as err:
                print(err)
                logging.info("failed connection, retrying in 1 minute")
                time.sleep(60)
            except Exception as err:
                self.connector.main_connection.rollback()
                raise err

        print('stop')
        raise NotImplementedError()

    def create_params_for_table_fill(self, tables_to_fill, connection):
        param_dict = {}
        for table_to_fill in tables_to_fill:
            param_dict[table_to_fill + '_fill'] = True
            param_dict[table_to_fill + '_cursor'] = ''
        self.connector.create_params(param_dict, connection)

    def sync_assetrelaties(self):
        start = time.time()
        assetrelatie_syncer = AssetRelatiesSyncer(em_infra_importer=self.eminfra_importer,
                                                  post_gis_connector=self.connector)
        while True:
            try:
                params = self.connector.get_params()
                assetrelatie_syncer.sync_assetrelaties(pagingcursor=params['pagingcursor'])
            except AssetMissingError as exc:
                self.events_processor.postgis_connector.connection.rollback()
                missing_assets = exc.args[0]
                current_paging_cursor = self.eminfra_importer.pagingcursor
                self.eminfra_importer.pagingcursor = ''
                processor = NieuwAssetProcessor(cursor=self.connector.connection.cursor(),
                                                em_infra_importer=self.eminfra_importer)
                processor.process(missing_assets)
                self.eminfra_importer.pagingcursor = current_paging_cursor
                self.events_processor.postgis_connector.save_props_to_params({'pagingcursor': current_paging_cursor})

            if self.eminfra_importer.pagingcursor == '':
                break

        end = time.time()
        logging.info(f'time for all assetrelaties: {round(end - start, 2)}')

    def sync_betrokkenerelaties(self, page_size, pagingcursor):
        # logging.info(f'Filling beheerders table')
        # start = time.time()
        # beheerder_syncer = BeheerderSyncer(em_infra_importer=self.eminfra_importer, postgis_connector=self.connector,
        #                                    resource='beheerders')
        # connection = self.connector.get_connection()
        # beheerder_syncer.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
        # self.connector.update_params(params={'beheerders_fill': False}, connection=connection)
        # self.connector.kill_connection(connection)
        # end = time.time()
        # logging.info(f'Time for all beheerders: {round(end - start, 2)}')

        logging.info(f'Filling betrokkenerelaties table')
        start = time.time()
        betrokkenerelatie_syncer = BetrokkeneRelatiesFiller(
            eminfra_importer=self.eminfra_importer, postgis_connector=self.connector, resource='betrokkenerelaties')
        connection = self.connector.get_connection()
        params = None
        while True:
            try:
                params = self.connector.get_params(connection)
                betrokkenerelatie_syncer.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
            except AgentMissingError as exc:
                missing_agents = exc.args[0]
                params = self.connector.get_params(connection)
                if 'agents_fill' in params and params['agents_fill']:
                    time.sleep(30)
                    continue

                logging.info(f'Syncing {len(missing_agents)} agents first.')
                self.connector.create_params(params={'agents_ad_hoc': ''}, connection=connection)
                agent_syncer = AgentSyncer(eminfra_importer=self.eminfra_importer, postgis_connector=self.connector,
                                           resource='agents')
                agent_syncer.sync(missing_agents, connection=connection)
                self.connector.delete_params(params={'agents_ad_hoc': ''}, connection=connection)
                continue

                # make different setup for syncing specific object

                # create param agents_ad_hoc_random_nr
                # pas it as paging cursor variable
                # get all agents by uuids
                # clean up by removing that param

                # Updater : updates objects
                #   logic how to update/create/delete objects
                #   SQL query's
                # Syncers : converts uuids to generator to be processed by Updater
                #   contains logic on how to convert uuids to dicts
                # Fillers : creates a generator to loop over all objects, to be processed by Updater
                # Fillers inherit from FastFiller, so they all use the same mechanic fill()
                #   contains logic on how to get all dicts



                # TODO change to:
                # get params
                # if agents_sync is still running:
                # wait a number of seconds
                # else import the missing agents

                # current_paging_cursor = self.eminfra_importer.pagingcursor
                # self.eminfra_importer.pagingcursor = ''
                # self.fill_agents(page_size=params['pagesize'], pagingcursor='')
                # self.eminfra_importer.pagingcursor = current_paging_cursor
                # self.connector.save_props_to_params({'pagingcursor': current_paging_cursor}, connection)

            if self.eminfra_importer.paging_cursors['betrokkenerelaties_cursor'] == '':
                break

        end = time.time()
        logging.info(f'time for all betrokkenerelaties: {round(end - start, 2)}')

    def sync_bestekkoppelingen(self):
        start = time.time()
        bestek_koppeling_syncer = BestekKoppelingSyncer(em_infra_importer=self.eminfra_importer,
                                                        postGIS_connector=self.connector)
        bestek_koppeling_syncer.sync_bestekkoppelingen()
        end = time.time()
        logging.info(f'time for all bestekkoppelingen: {round(end - start, 2)}')

    def sync_assets(self, page_size, pagingcursor):
        start = time.time()
        asset_syncer = AssetSyncer(em_infra_importer=self.eminfra_importer,
                                   postgis_connector=self.connector)
        params = None
        while True:
            try:
                params = self.connector.get_params()
                asset_syncer.sync_assets(pagingcursor=params['pagingcursor'])
            except (AssetTypeMissingError, AttribuutMissingError):
                self.connector.connection.rollback()
                print('refreshing assettypes')
                current_paging_cursor = self.eminfra_importer.pagingcursor
                self.eminfra_importer.pagingcursor = ''
                self.sync_assettypes(page_size=params['pagesize'], pagingcursor='')
                self.eminfra_importer.pagingcursor = current_paging_cursor
                self.connector.save_props_to_params({'pagingcursor': current_paging_cursor})
            except (ToezichtgroepMissingError):
                self.connector.connection.rollback()
                print('refreshing toezichtgroepen')
                current_paging_cursor = self.eminfra_importer.pagingcursor
                self.eminfra_importer.pagingcursor = ''
                self.sync_toezichtgroepen(page_size=params['pagesize'], pagingcursor='')
                self.eminfra_importer.pagingcursor = current_paging_cursor
                self.connector.save_props_to_params({'pagingcursor': current_paging_cursor})

            if self.eminfra_importer.pagingcursor == '':
                break

        end = time.time()
        logging.info(f'time for all assets: {round(end - start, 2)}')

    def sync_relatietypes(self):
        start = time.time()
        relatietype_syncer = RelatietypeSyncer(em_infra_importer=self.eminfra_importer,
                                               postgis_connector=self.connector)
        relatietype_syncer.sync_relatietypes()
        end = time.time()
        logging.info(f'time for all relatietypes: {round(end - start, 2)}')

    def sync_assettypes(self, page_size, pagingcursor):
        start = time.time()
        assettype_syncer = AssetTypeSyncer(emInfraImporter=self.eminfra_importer,
                                           postGIS_connector=self.connector)
        assettype_syncer.sync_assettypes(pagingcursor=pagingcursor, page_size=page_size)
        end = time.time()
        logging.info(f'time for all assettypes: {round(end - start, 2)}')

    def sync_bestekken(self, page_size, pagingcursor):
        start = time.time()
        bestek_syncer = BestekSyncer(em_infra_importer=self.eminfra_importer,
                                     postGIS_connector=self.connector)
        bestek_syncer.sync_bestekken(pagingcursor=pagingcursor, page_size=page_size)
        end = time.time()
        logging.info(f'time for all bestekken: {round(end - start, 2)}')

    def sync_beheerders(self, page_size, pagingcursor):
        logging.info(f'Filling beheerders table')
        start = time.time()
        beheerder_syncer = BeheerderSyncer(em_infra_importer=self.eminfra_importer, postgis_connector=self.connector,
                                           resource='beheerders')
        connection = self.connector.get_connection()
        beheerder_syncer.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
        self.connector.update_params(params={'beheerders_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'Time for all beheerders: {round(end - start, 2)}')

    def sync_identiteiten(self, page_size, pagingcursor):
        start = time.time()
        identiteit_syncer = IdentiteitSyncer(em_infra_importer=self.eminfra_importer, postgis_connector=self.connector)
        identiteit_syncer.sync_identiteiten(pagingcursor=pagingcursor, page_size=page_size)
        end = time.time()
        logging.info(f'time for all identiteiten: {round(end - start, 2)}')

    def sync_toezichtgroepen(self, page_size, pagingcursor):
        logging.info(f'Filling toezichtgroepen table')
        start = time.time()
        toezichtgroep_syncer = ToezichtgroepSyncer(em_infra_importer=self.eminfra_importer,
                                                   postgis_connector=self.connector, resource='toezichtgroepen')
        connection = self.connector.get_connection()
        toezichtgroep_syncer.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
        self.connector.update_params(params={'toezichtgroepen_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'Time for all toezichtgroepen: {round(end - start, 2)}')

    def fill_agents(self, page_size, pagingcursor):
        logging.info(f'Filling agents table')
        start = time.time()
        agent_syncer = AgentFiller(eminfra_importer=self.eminfra_importer, postgis_connector=self.connector, resource='agents')
        connection = self.connector.get_connection()
        agent_syncer.fill(pagingcursor=pagingcursor, page_size=page_size, connection=connection)
        self.connector.update_params(params={'agents_fill': False}, connection=connection)
        self.connector.kill_connection(connection)
        end = time.time()
        logging.info(f'Time for all agents: {round(end - start, 2)}')

    def save_last_feedevent_to_params(self, page_size: int, feed: str):
        start_num = 1
        step = 5
        start_num = self.recur_exp_find_start_page(current_num=start_num, step=step, page_size=page_size, feed=feed)
        current_page_num = self.recur_find_last_page(current_num=int(start_num / step),
                                                     current_step=int(start_num / step),
                                                     step=step, page_size=page_size, feed=feed)

        # doublecheck
        event_page = self.eminfra_importer.get_events_from_feedpage(page_num=current_page_num, page_size=page_size,
                                                                    feed=feed)
        links = event_page['links']
        prev_link = next((l for l in links if l['rel'] == 'previous'), None)
        if prev_link is not None:
            raise RuntimeError('algorithm did not result in the last page')

        # find last event_id
        entries = event_page['entries']
        last_event_uuid = entries[0]['id']

        self.connector.create_params(
            {f'event_uuid_{feed}': last_event_uuid,
             f'page_{feed}': current_page_num},
            connection=self.connector.main_connection)
        logging.info(f'Added last page of current feed for {feed} to params (page: {current_page_num})')

    def recur_exp_find_start_page(self, current_num, step, page_size, feed):
        event_page = None
        try:
            event_page = self.eminfra_importer.get_events_from_feedpage(page_num=current_num, page_size=page_size,
                                                                        feed=feed)
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
                self.eminfra_importer.get_events_from_feedpage(page_num=new_num, page_size=page_size, feed=feed)
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
