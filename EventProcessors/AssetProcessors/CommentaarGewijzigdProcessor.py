import logging
import time

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor


class CommentaarGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started updating commentaar')
        start = time.time()

        asset_dicts = self.eminfra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        values, amount = self.create_values_string_from_dicts(assets_dicts=asset_dicts)
        self.perform_update_with_values(connection=connection, values=values)

        end = time.time()
        logging.info(f'updated commentaar of {amount} asset(s) in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def create_values_string_from_dicts(assets_dicts):
        values = ''
        counter = 0
        for asset_dict in assets_dicts:
            counter += 1
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]

            notitie = None
            if 'AIMObject.notitie' in asset_dict:
                notitie = asset_dict['AIMObject.notitie']
            values += f"('{uuid}',"

            if notitie is None:
                values += 'NULL'
            else:
                notitie = notitie.replace("'","''")
                values += f"'{notitie}'"
            values = values + '),'
        return values, counter

    @staticmethod
    def perform_update_with_values(connection, values):
        update_query = f"""
        WITH s (uuid, commentaar)  
            AS (VALUES {values[:-1]}),
        to_update AS (
            SELECT uuid::uuid AS uuid, commentaar FROM s)
        UPDATE assets 
        SET commentaar = to_update.commentaar
        FROM to_update 
        WHERE to_update.uuid = assets.uuid;"""
        with connection.cursor() as cursor:
            cursor.execute(update_query)
