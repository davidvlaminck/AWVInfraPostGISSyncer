import logging
import time

from BestekKoppelingSyncer import BestekKoppelingSyncer
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor


class BestekGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started updating bestekken')
        start = time.time()

        koppelingen_list = []
        asset_uuid_list = []
        koppelingen_generator = self.eminfra_importer.get_all_bestekkoppelingen_from_webservice_by_asset_uuids(asset_uuids=uuids)
        for asset_uuid, koppelingen in koppelingen_generator:
            asset_uuid_list.append(asset_uuid)
            koppelingen_list.append(list(koppelingen))
        BestekKoppelingSyncer.update_bestekkoppelingen_by_asset_uuids(connection=connection, asset_uuids=asset_uuid_list,
                                                                      bestek_koppelingen_dicts_list=koppelingen_list)

        # asset_dicts = self.eminfra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        # values = NieuwAssetProcessor.create_values_string_from_dicts(connection=connection, assets_dicts=asset_dicts)
        # NieuwAssetProcessor.perform_insert_with_values(connection=connection, values=values)

        end = time.time()
        logging.info(f'updated {len(uuids)} assets in {str(round(end - start, 2))} seconds.')
