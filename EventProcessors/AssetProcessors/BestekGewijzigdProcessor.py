import logging
import time

from BestekKoppelingSyncer import BestekKoppelingSyncer
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor


class BestekGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started updating bestekkoppelingen')
        start = time.time()

        koppelingen_list = []
        asset_uuid_list = []

        uuids_string = "'::uuid,'".join(uuids)
        get_asset_cat_query = "SELECT a.uuid, CASE WHEN t.uri LIKE '%/ns/onderdeel#%' THEN 'onderdelen' ELSE 'installaties' END AS asset_cat " \
                              "FROM assets a " \
                              "LEFT JOIN assettypes t ON a.assettype = t.uuid " \
                              f"WHERE a.uuid in ('{uuids_string}'::uuid);"
        with connection.cursor() as cursor:
            start = time.time()
            cursor.execute(get_asset_cat_query)
            all_rows = cursor.fetchall()
            onderdelen_to_update = list(map(lambda x: x[0], filter(lambda x: x[1] == 'onderdelen', all_rows)))
            installaties_to_update = list(map(lambda x: x[0], filter(lambda x: x[1] == 'installaties', all_rows)))

            amount = 0
            koppelingen_generator = self.eminfra_importer.\
                get_all_bestekkoppelingen_from_webservice_by_asset_uuids_onderdelen(asset_uuids=onderdelen_to_update)
            for asset_uuid, koppelingen in koppelingen_generator:
                amount += 1
                asset_uuid_list.append(asset_uuid)
                koppelingen_list.append(list(koppelingen))

            koppelingen_generator = self.eminfra_importer. \
                get_all_bestekkoppelingen_from_webservice_by_asset_uuids_installaties(asset_uuids=installaties_to_update)
            for asset_uuid, koppelingen in koppelingen_generator:
                amount += 1
                asset_uuid_list.append(asset_uuid)
                koppelingen_list.append(list(koppelingen))

        BestekKoppelingSyncer.update_bestekkoppelingen_by_asset_uuids(connection=connection,
                                                                      asset_uuids=asset_uuid_list,
                                                                      bestek_koppelingen_dicts_list=koppelingen_list)

        end = time.time()
        logging.info(f'updated bestekkoppelingen of {amount} asset(s) in {str(round(end - start, 2))} seconds.')
