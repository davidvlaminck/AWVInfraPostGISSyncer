import logging
import time

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor


class VplanGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started updating vplan')
        start = time.time()

        vplannen_generator = self.eminfra_importer.get_all_vplankoppelingen_from_webservice_by_asset_uuids(
            asset_uuids=uuids)
        for asset_uuid, vplan_dict in vplannen_generator:
            self.update_vplankoppelingen_by_asset_uuid(connection=connection, asset_uuid=asset_uuid,
                                                  vplan_dict=list(vplan_dict))

        end = time.time()
        logging.info(f'updated vplan for up to {len(uuids)} assets in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def update_vplankoppelingen_by_asset_uuid(connection, asset_uuid, vplan_dict):
        raise NotImplementedError
        delete_query = f"DELETE FROM public.vplan_koppelingen WHERE assetUuid = '{asset_uuid}';"
        with connection.cursor() as cursor:
            cursor.execute(delete_query)
            if 'vplanRef' not in vplan_dict[0]:
                return
            single_aansluiting_dict = vplan_dict[0]['vplanRef']
            ean = single_aansluiting_dict.get('ean', None)
            aansluitnummer = single_aansluiting_dict.get('aansluitnummer', None)
            insert_query = f"""INSERT INTO vplan_koppelingen (assetUuid, EAN, aansluiting) 
            VALUES ('{asset_uuid}'"""
            for value in [ean, aansluitnummer]:
                if value is None:
                    insert_query += ",NULL"
                else:
                    insert_query += f",'{value}'"
            cursor.execute(insert_query + ");")
