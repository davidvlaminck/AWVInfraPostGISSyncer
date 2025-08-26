import logging
import time

from AssetRelatiesUpdater import AssetRelatiesUpdater
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Helpers import chunked


class NieuwAssetrelatieProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info('started creating assetrelaties')
        start = time.time()

        assetrelatie_count = 0
        for uuids_chunk in chunked(uuids, 100):
            generator = self.eminfra_importer.import_resource_from_webservice_by_uuids(uuids=uuids_chunk,
                                                                                       resource='assetrelaties')

            assetrelatie_count += AssetRelatiesUpdater.update_objects(
                object_generator=generator, connection=connection)

        end = time.time()
        logging.info(f'created {assetrelatie_count} assetrelaties in {str(round(end - start, 2))} seconds.')
