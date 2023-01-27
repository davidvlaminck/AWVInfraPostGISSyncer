import logging
import time

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor


class ElekAansluitingGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started updating elek aansluiting')
        start = time.time()

        aansluitingen_generator = self.eminfra_importer.get_all_elek_aansluitingen_from_webservice_by_asset_uuids(
            asset_uuids=uuids)
        for asset_uuid, aansluiting_dict in aansluitingen_generator:
            self.update_aansluiting_by_asset_uuid(connection=connection, asset_uuid=asset_uuid,
                                                  aansluiting_dict=list(aansluiting_dict))

        end = time.time()
        logging.info(f'updated elek aansluiting for up to {len(uuids)} assets in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def update_aansluiting_by_asset_uuid(connection, asset_uuid, aansluiting_dict):
        delete_query = f"DELETE FROM public.elek_aansluitingen WHERE assetUuid = '{asset_uuid}';"
        with connection.cursor() as cursor:
            cursor.execute(delete_query)
            if 'elektriciteitsAansluitingRef' not in aansluiting_dict[0]:
                return
            single_aansluiting_dict = aansluiting_dict[0]['elektriciteitsAansluitingRef']
            ean = single_aansluiting_dict.get('ean', None)
            aansluitnummer = single_aansluiting_dict.get('aansluitnummer', None)
            insert_query = f"""INSERT INTO elek_aansluitingen (assetUuid, EAN, aansluiting) 
            VALUES ('{asset_uuid}'"""
            for value in [ean, aansluitnummer]:
                if value is None:
                    insert_query += ",NULL"
                else:
                    insert_query += f",'{value}'"
            cursor.execute(insert_query + ");")
