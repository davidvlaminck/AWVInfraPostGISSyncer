import logging
import time

from BestekKoppelingSyncer import BestekKoppelingSyncer
from EventProcessors.NieuwAssetProcessor import NieuwAssetProcessor
from EventProcessors.SpecificEventProcessor import SpecificEventProcessor


class BestekGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, eminfra_importer):
        super().__init__(cursor, eminfra_importer)

    def process(self, uuids: [str]):
        logging.info(f'started updating bestekken')
        start = time.time()

        koppelingen_list = []
        asset_uuid_list = []
        koppelingen_generator = self.em_infra_importer.get_all_bestekkoppelingen_from_webservice_by_asset_uuids(asset_uuids=uuids)
        for asset_uuid, koppelingen in koppelingen_generator:
            asset_uuid_list.append(asset_uuid)
            koppelingen_list.append(list(koppelingen))
        BestekKoppelingSyncer.update_bestekkoppelingen_by_asset_uuids(cursor=self.cursor, asset_uuids=asset_uuid_list,
                                                                      bestek_koppelingen_dicts_list=koppelingen_list)

        asset_dicts = self.em_infra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        values = NieuwAssetProcessor.create_values_string_from_dicts(cursor=self.cursor, assets_dicts=asset_dicts)
        NieuwAssetProcessor.perform_insert_with_values(cursor=self.cursor, values=values)

        end = time.time()
        logging.info(f'updated {len(asset_dicts)} assets in {str(round(end - start, 2))} seconds.')
