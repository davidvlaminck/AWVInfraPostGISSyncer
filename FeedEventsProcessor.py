import logging
import time

from EMInfraImporter import EMInfraImporter
from EventProcessorFactory import EventProcessorFactory
from PostGISConnector import PostGISConnector


class FeedEventsProcessor:
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter):
        self.postgis_connector = postgis_connector
        self.eminfra_importer = eminfra_importer
        self.resource: str = ''

    def process_events(self, event_params: (), connection):
        self.process_events_by_event_params(event_params=event_params, connection=connection)

        self.postgis_connector.update_params(connection=connection,
                                             params={f'page_{self.resource}': event_params.page_num,
                                                     f'event_uuid_{self.resource}': event_params.event_uuid})
        connection.commit()

    def process_events_by_event_params(self, event_params, connection):
        event_dict = event_params.event_dict

        # process events of this type before moving on to the other events
        process_first_list = ['NIEUW_ONDERDEEL', 'NIEUWE_INSTALLATIE', 'NIEUWE_AGENT', 'NIEUWE_RELATIE']
        for process_first_event in process_first_list:
            if process_first_event in event_dict.keys() and len(event_dict[process_first_event]) > 0:
                event_processor = self.create_processor(process_first_event)
                start = time.time()
                event_processor.process(event_dict[process_first_event], connection=connection)
                end = time.time()
                avg = round((end - start) / len(event_params.event_dict[process_first_event]), 2)
                logging.info(
                    f'finished processing events of type {process_first_event} in {str(round(end - start, 2))} seconds.'
                    f' Average time per item = {str(avg)} seconds')

        for event_type, uuids in event_dict.items():
            if event_type in process_first_list or len(uuids) == 0:
                continue
            event_processor = self.create_processor(event_type)
            if event_processor is None:
                continue
            start = time.time()
            event_processor.process(uuids, connection=connection)
            end = time.time()
            avg = round((end - start) / len(uuids), 2)
            logging.info(
                f'finished processing events of type {event_type} in {str(round(end - start, 2))} seconds. Average time per item = {str(avg)} seconds')

    def create_processor(self, event_type):
        event_processor = EventProcessorFactory.create_event_processor(event_type=event_type,
                                                                       resource=self.resource,
                                                                       eminfra_importer=self.eminfra_importer,
                                                                       postgis_connector=self.postgis_connector)
        return event_processor
