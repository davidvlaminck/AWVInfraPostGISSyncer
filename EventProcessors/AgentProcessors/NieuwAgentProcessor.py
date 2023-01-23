import logging
import time

from AgentUpdater import AgentUpdater
from EventProcessors.SpecificEventProcessor import SpecificEventProcessor

from Helpers import chunked


class NieuwAgentProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started creating agents')
        start = time.time()

        agent_count = 0
        for uuids in chunked(uuids, 100):
            generator = self.em_infra_importer.import_resource_from_webservice_by_uuids(uuids=uuids,
                                                                                        resource='agents')

            agent_count += AgentUpdater.update_objects(object_generator=generator, connection=connection,
                                                       insert_only=True)

        end = time.time()
        logging.info(f'created {agent_count} agents in {str(round(end - start, 2))} seconds.')
