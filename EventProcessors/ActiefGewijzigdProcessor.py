import logging
import time

from EventProcessors.SpecificEventProcessor import SpecificEventProcessor


class ActiefGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, eminfra_importer):
        super().__init__(cursor, eminfra_importer)

    def process(self, uuids: [str]):
        logging.info(f'started updating actief')
        start = time.time()

        asset_dicts = self.em_infra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        values = self.create_values_string_from_dicts(assets_dicts=asset_dicts)
        self.perform_update_with_values(cursor=self.cursor, values=values)

        end = time.time()
        logging.info(f'updated {len(asset_dicts)} assets in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def create_values_string_from_dicts(assets_dicts):
        values = ''
        for asset_dict in assets_dicts:
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]
            actief = asset_dict['AIMDBStatus.isActief']
            values += f"('{uuid}',{actief}),"

        return values

    @staticmethod
    def perform_update_with_values(cursor, values):
        update_query = f"""
        WITH s (uuid, actief)  
            AS (VALUES {values[:-1]}),
        to_update AS (
            SELECT uuid::uuid AS uuid, actief FROM s)
        UPDATE assets 
        SET actief = to_update.actief
        FROM to_update 
        WHERE to_update.uuid = assets.uuid;"""
        cursor.execute(update_query)
