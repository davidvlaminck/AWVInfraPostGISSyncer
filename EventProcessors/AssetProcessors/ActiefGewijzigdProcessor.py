import logging
import time

from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor


class ActiefGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer: EMInfraImporter):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started updating actief')
        start = time.time()

        asset_dicts = self.eminfra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        values, amount = self.create_values_string_from_dicts(assets_dicts=asset_dicts)
        self.perform_update_with_values(connection=connection, values=values)

        end = time.time()
        logging.info(f'updated actief of {amount} asset(s) in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def create_values_string_from_dicts(assets_dicts):
        values = ''
        counter = 0
        for asset_dict in assets_dicts:
            counter += 1
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]
            actief = asset_dict.get('AIMDBStatus.isActief', True)
            values += f"('{uuid}',{actief}),"

        return values, counter

    @staticmethod
    def perform_update_with_values(connection, values):
        with connection.cursor() as cursor:
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
