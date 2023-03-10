import logging
import time

from BetrokkeneRelatiesUpdater import BetrokkeneRelatiesUpdater
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor

from Helpers import chunked


class NieuwBetrokkenerelatieProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info('started creating betrokkenerelaties')
        start = time.time()

        betrokkenerelatie_count = 0
        for uuids in chunked(uuids, 100):
            generator = self.eminfra_importer.import_resource_from_webservice_by_uuids(uuids=uuids,
                                                                                       resource='betrokkenerelaties')

            betrokkenerelatie_count += BetrokkeneRelatiesUpdater.update_objects(
                object_generator=generator, connection=connection)

        end = time.time()
        logging.info(f'created {betrokkenerelatie_count} betrokkenerelaties in {str(round(end - start, 2))} seconds.')
