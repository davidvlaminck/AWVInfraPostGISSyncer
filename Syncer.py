import logging
import time
import traceback
from datetime import datetime

import requests

from AgentSyncer import AgentSyncer
from AssetRelatiesSyncer import AssetRelatiesSyncer
from AssetSyncer import AssetSyncer
from AssetTypeSyncer import AssetTypeSyncer
from BeheerderSyncer import BeheerderSyncer
from BestekKoppelingSyncer import BestekKoppelingSyncer
from BestekSyncer import BestekSyncer
from BetrokkeneRelatiesSyncer import BetrokkeneRelatiesSyncer
from EMInfraImporter import EMInfraImporter
from FeedEventsCollector import FeedEventsCollector
from FeedEventsProcessor import FeedEventsProcessor
from IdentiteitSyncer import IdentiteitSyncer
from PostGISConnector import PostGISConnector
from RelatietypeSyncer import RelatietypeSyncer
from RequestHandler import RequestHandler
from ToezichtgroepSyncer import ToezichtgroepSyncer


class Syncer:
    def __init__(self, connector: PostGISConnector, request_handler: RequestHandler, eminfra_importer: EMInfraImporter,
                 settings=None):
        self.connector = connector
        self.request_handler = request_handler
        self.eminfra_importer = eminfra_importer
        self.events_collector = FeedEventsCollector(eminfra_importer)
        self.events_processor = FeedEventsProcessor(connector, eminfra_importer)
        self.settings = settings
        self.sync_start = None
        self.sync_end = None
        if 'time' in self.settings:
            self.sync_start = self.settings['time']['start']
            self.sync_end = self.settings['time']['end']

    def start_syncing(self):
        while True:
            try:
                params = self.connector.get_params()
                if params is None:
                    self.connector.set_up_tables()
                    params = self.connector.get_params()

                if params['fresh_start']:
                    self.perform_fresh_start_sync(params)
                else:
                    self.perform_syncing()
            except requests.exceptions.ConnectionError as exc:
                print(exc)

    def perform_fresh_start_sync(self, params: dict):
        page_size = params['pagesize']
        page = params['page']
        if page == -1:
            self.save_last_feedevent_to_params(page_size)

        while True:
            try:
                # main sync loop for a fresh start
                params = self.connector.get_params()
                sync_step = params['sync_step']
                pagingcursor = params['pagingcursor']
                page_size = params['pagesize']

                if sync_step == -1:
                    sync_step = 1
                if sync_step >= 15:
                    break

                if sync_step == 1:
                    start = time.time()
                    agent_syncer = AgentSyncer(emInfraImporter=self.eminfra_importer, postGIS_connector=self.connector)
                    agent_syncer.sync_agents(pagingcursor=pagingcursor, page_size=page_size)
                    end = time.time()
                    logging.info(f'time for all agents: {round(end - start, 2)}')
                elif sync_step == 2:
                    start = time.time()
                    toezichtgroep_syncer = ToezichtgroepSyncer(emInfraImporter=self.eminfra_importer, postGIS_connector=self.connector)
                    toezichtgroep_syncer.sync_toezichtgroepen(pagingcursor=pagingcursor, page_size=page_size)
                    end = time.time()
                    logging.info(f'time for all toezichtgroepen: {round(end - start, 2)}')
                elif sync_step == 3:
                    start = time.time()
                    identiteit_syncer = IdentiteitSyncer(em_infra_importer=self.eminfra_importer, postgis_connector=self.connector)
                    identiteit_syncer.sync_identiteiten(pagingcursor=pagingcursor, page_size=page_size)
                    end = time.time()
                    logging.info(f'time for all identiteiten: {round(end - start, 2)}')
                elif sync_step == 4:
                    start = time.time()
                    beheerder_syncer = BeheerderSyncer(em_infra_importer=self.eminfra_importer, postgis_connector=self.connector)
                    beheerder_syncer.sync_beheerders(pagingcursor=pagingcursor, page_size=page_size)
                    end = time.time()
                    logging.info(f'time for all beheerders: {round(end - start, 2)}')
                elif sync_step == 5:
                    start = time.time()
                    bestek_syncer = BestekSyncer(em_infra_importer=self.eminfra_importer,
                                                 postGIS_connector=self.connector)
                    bestek_syncer.sync_bestekken(pagingcursor=pagingcursor, page_size=page_size)
                    end = time.time()
                    logging.info(f'time for all bestekken: {round(end - start, 2)}')
                elif sync_step == 6:
                    start = time.time()
                    assettype_syncer = AssetTypeSyncer(emInfraImporter=self.eminfra_importer,
                                                       postGIS_connector=self.connector)
                    assettype_syncer.sync_assettypes(pagingcursor=pagingcursor, page_size=page_size)
                    end = time.time()
                    logging.info(f'time for all assettypes: {round(end - start, 2)}')
                elif sync_step == 7:
                    start = time.time()
                    relatietype_syncer = RelatietypeSyncer(em_infra_importer=self.eminfra_importer,
                                                           postgis_connector=self.connector)
                    relatietype_syncer.sync_relatietypes()
                    end = time.time()
                    logging.info(f'time for all relatietypes: {round(end - start, 2)}')
                elif sync_step == 8:
                    start = time.time()
                    asset_syncer = AssetSyncer(em_infra_importer=self.eminfra_importer,
                                               postgis_connector=self.connector)
                    asset_syncer.sync_assets(pagingcursor=pagingcursor, page_size=page_size)
                    end = time.time()
                    logging.info(f'time for all assets: {round(end - start, 2)}')
                elif sync_step == 9:
                    start = time.time()
                    bestek_koppeling_syncer = BestekKoppelingSyncer(em_infra_importer=self.eminfra_importer,
                                                                    postGIS_connector=self.connector)
                    bestek_koppeling_syncer.sync_bestekkoppelingen()
                    end = time.time()
                    logging.info(f'time for all bestekkoppelingen: {round(end - start, 2)}')
                elif sync_step == 10:
                    start = time.time()
                    betrokkenerelatie_syncer = BetrokkeneRelatiesSyncer(em_infra_importer=self.eminfra_importer,
                                                                        post_gis_connector=self.connector)
                    betrokkenerelatie_syncer.sync_betrokkenerelaties()
                    end = time.time()
                    logging.info(f'time for all betrokkenerelaties: {round(end - start, 2)}')
                elif sync_step == 11:
                    start = time.time()
                    assetrelatie_syncer = AssetRelatiesSyncer(em_infra_importer=self.eminfra_importer,
                                                              post_gis_connector=self.connector)
                    assetrelatie_syncer.sync_assetrelaties()
                    end = time.time()
                    logging.info(f'time for all assetrelaties: {round(end - start, 2)}')
                else:
                    # TODO documenten
                    raise NotImplementedError

                pagingcursor = self.eminfra_importer.pagingcursor
                if pagingcursor == '':
                    sync_step += 1
                self.connector.save_props_to_params(
                    {'sync_step': sync_step,
                     'pagingcursor': pagingcursor})
                if sync_step >= 12:
                    self.connector.save_props_to_params(
                        {'fresh_start': False})
                self.connector.connection.commit()
            except ConnectionError as err:
                print(err)
                logging.info("failed connection, retrying in 1 minute")
                time.sleep(60)

    def save_last_feedevent_to_params(self, page_size: int):
        start_num = 1
        step = 5
        start_num = self.recur_exp_find_start_page(current_num=start_num, step=step, page_size=page_size)
        current_page_num = self.recur_find_last_page(current_num=int(start_num / step),
                                                     current_step=int(start_num / step),
                                                     step=step, page_size=page_size)

        # doublecheck
        event_page = self.eminfra_importer.get_events_from_page(page_num=current_page_num, page_size=page_size)
        links = event_page['links']
        prev_link = next((l for l in links if l['rel'] == 'previous'), None)
        if prev_link is not None:
            raise RuntimeError('algorithm did not result in the last page')

        # find last event_id
        entries = event_page['entries']
        last_event_uuid = entries[0]['id']

        self.connector.save_props_to_params(
            {'event_uuid': last_event_uuid,
             'page': current_page_num})

    def recur_exp_find_start_page(self, current_num, step, page_size):
        event_page = self.eminfra_importer.get_events_from_page(page_num=current_num, page_size=page_size)
        if 'message' not in event_page:
            return self.recur_exp_find_start_page(current_num=current_num * step, step=step, page_size=100)
        return current_num

    def recur_find_last_page(self, current_num, current_step, step, page_size):
        new_i = 0
        for i in range(step + 1):
            new_num = current_num + current_step * i
            event_page = self.eminfra_importer.get_events_from_page(page_num=new_num, page_size=page_size)
            if 'message' in event_page:
                new_i = i - 1
                break
        if current_step == 1:
            return current_num + current_step * new_i

        return self.recur_find_last_page(current_num + current_step * new_i,
                                         int(current_step / step), step, page_size)

    def calculate_sync_allowed_by_time(self):
        if self.sync_start is None:
            return True

        start_struct = time.strptime(self.sync_start, "%H:%M:%S")
        end_struct = time.strptime(self.sync_end, "%H:%M:%S")
        now = datetime.utcnow().time()
        start = now.replace(hour=start_struct.tm_hour, minute=start_struct.tm_min, second=start_struct.tm_sec)
        end = now.replace(hour=end_struct.tm_hour, minute=end_struct.tm_min, second=end_struct.tm_sec)
        v = start < now < end
        return v

    def perform_syncing(self):
        sync_allowed_by_time = self.calculate_sync_allowed_by_time()

        while sync_allowed_by_time:
            params = self.connector.get_params()
            current_page = params['page']
            completed_event_id = params['event_uuid']
            page_size = params['pagesize']
            logging.info(f'starting a sync cycle, page: {str(current_page + 1)} event_uuid: {str(completed_event_id)}')
            start = time.time()

            eventsparams_to_process = self.events_collector.collect_starting_from_page(current_page, completed_event_id,
                                                                                       page_size)

            total_events = sum(len(lists) for lists in eventsparams_to_process.event_dict.values())
            if total_events == 0:
                logging.info(f"The database is fully synced. Continuing keep up to date in 30 seconds")
                self.connector.save_props_to_params({'last_update_utc': datetime.utcnow()})
                time.sleep(30)  # wait 30 seconds to prevent overloading API
                continue

            end = time.time()

            self.log_eventparams(eventsparams_to_process.event_dict, round(end - start, 2))
            try:
                self.events_processor.process_events(eventsparams_to_process)
            except Exception as exc:
                traceback.print_exception(exc)
                self.events_processor.postgis_connector.connection.rollback()

            sync_allowed_by_time = self.calculate_sync_allowed_by_time()

    @staticmethod
    def log_eventparams(event_dict, time: float):
        total = sum(len(events) for events in event_dict.values())
        logging.info(f'fetched {total} asset events to sync in {time} seconds')
        for k, v in event_dict.items():
            if len(v) > 0:
                logging.info(f'number of events of type {k}: {len(v)}')
