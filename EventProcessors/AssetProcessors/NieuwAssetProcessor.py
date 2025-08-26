import logging
import time

from AssetUpdater import AssetUpdater
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Helpers import chunked


class NieuwAssetProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info('started creating assets')
        start = time.time()

        asset_count = 0
        for uuids_chunk in chunked(uuids, 100):
            generator = self.eminfra_importer.import_resource_from_webservice_by_uuids(uuids=uuids_chunk,
                                                                                       resource='assets')

            asset_count += AssetUpdater.update_objects(object_generator=generator, connection=connection,
                                                       insert_only=True, eminfra_importer=self.eminfra_importer)

        end = time.time()
        logging.info(f'created {asset_count} assets in {str(round(end - start, 2))} seconds.')
