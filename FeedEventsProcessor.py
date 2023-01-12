import logging
import time

import psycopg2

from EMInfraImporter import EMInfraImporter
from EventProcessorFactory import EventProcessorFactory
from PostGISConnector import PostGISConnector


class FeedEventsProcessor:
    def __init__(self, postgis_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postgis_connector = postgis_connector
        self.emInfraImporter = em_infra_importer

    def process_events(self, event_params: ()):

        cursor = self.postgis_connector.connection.cursor()
        self.process_events_by_event_params(event_params=event_params, cursor=cursor)

        self.postgis_connector.save_props_to_params(cursor=cursor, params={'page': event_params.page_num,
                                                                           'event_uuid': event_params.event_uuid})
        self.postgis_connector.commit_transaction()

    def process_events_by_event_params(self, event_params, cursor: psycopg2._psycopg.cursor):
        event_dict = event_params.event_dict

        # make sure events NIEUW_ONDERDEEL and NIEUWE_INSTALLATIE are processed before any others
        if "NIEUW_ONDERDEEL" in event_dict.keys() and len(event_dict["NIEUW_ONDERDEEL"]) > 0:
            event_processor = self.create_processor("NIEUW_ONDERDEEL", cursor)
            start = time.time()
            event_processor.process(event_dict["NIEUW_ONDERDEEL"])
            end = time.time()
            avg = round((end - start) / len(event_params.event_dict["NIEUW_ONDERDEEL"]), 2)
            logging.info(
                f'finished processing events of type NIEUW_ONDERDEEL in {str(round(end - start, 2))} seconds. Average time per item = {str(avg)} seconds')
        if "NIEUWE_INSTALLATIE" in event_dict.keys() and len(event_dict["NIEUWE_INSTALLATIE"]) > 0:
            event_processor = self.create_processor("NIEUWE_INSTALLATIE", cursor)
            start = time.time()
            event_processor.process(event_dict["NIEUWE_INSTALLATIE"])
            end = time.time()
            avg = round((end - start) / len(event_params.event_dict["NIEUWE_INSTALLATIE"]), 2)
            logging.info(
                f'finished processing events of type NIEUWE_INSTALLATIE in {str(round(end - start, 2))} seconds. Average time per item = {str(avg)} seconds')
        for event_type, uuids in event_dict.items():
            if event_type in ["NIEUW_ONDERDEEL", "NIEUWE_INSTALLATIE"] or len(uuids) == 0:
                continue
            event_processor = self.create_processor(event_type, cursor)
            if event_processor is None:
                continue
            start = time.time()
            event_processor.process(uuids)
            end = time.time()
            avg = round((end - start) / len(uuids), 2)
            logging.info(
                f'finished processing events of type {event_type} in {str(round(end - start, 2))} seconds. Average time per item = {str(avg)} seconds')

    def create_processor(self, event_type, cursor):
        event_processor = EventProcessorFactory.create_event_processor(event_type=event_type,
                                                                       cursor=cursor,
                                                                       em_infra_importer=self.emInfraImporter,
                                                                       postgis_connector=self.postgis_connector)
        return event_processor
