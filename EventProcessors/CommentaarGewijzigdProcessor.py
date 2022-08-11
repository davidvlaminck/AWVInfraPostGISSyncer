import logging
import time

from EventProcessors.SpecificEventProcessor import SpecificEventProcessor


class CommentaarGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, em_infra_importer):
        super().__init__(cursor, em_infra_importer)

    def process(self, uuids: [str]):
        logging.info(f'started updating commentaar')
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

            notitie = None
            if 'AIMObject.notitie' in asset_dict:
                notitie = asset_dict['AIMObject.notitie']
            values += f"('{uuid}',"

            if notitie is None:
                values += 'NULL,'
            else:
                notitie = notitie.replace("'","''")
                values += f"'{notitie}'"
            values = values + '),'
        return values

    @staticmethod
    def perform_update_with_values(cursor, values):
        update_query = f"""
        WITH s (uuid, commentaar)  
            AS (VALUES {values[:-1]}),
        to_update AS (
            SELECT uuid::uuid AS uuid, commentaar FROM s)
        UPDATE assets 
        SET commentaar = to_update.commentaar
        FROM to_update 
        WHERE to_update.uuid = assets.uuid;"""
        cursor.execute(update_query)
