import logging
import time

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor


class ToestandGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started updating toestand')
        start = time.time()

        asset_dicts = self.eminfra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        values, amount = self.create_values_string_from_dicts(assets_dicts=list(asset_dicts))
        self.perform_update_with_values(connection=connection, values=values)

        end = time.time()
        logging.info(f'updated toestand of {amount} asset(s) in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def create_values_string_from_dicts(assets_dicts):
        values = ''
        counter = 0
        for asset_dict in assets_dicts:
            counter += 1
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]

            toestand = None
            if 'AIMToestand.toestand' in asset_dict:
                toestand = asset_dict['AIMToestand.toestand'].replace(
                    'https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/', '')
            values += f"('{uuid}',"

            if toestand is None:
                values += 'NULL'
            else:
                values += f"'{toestand}'"
            values = values + '),'
        return values, counter

    @staticmethod
    def perform_update_with_values(connection, values):
        update_query = f"""
        WITH s (uuid, toestand)  
            AS (VALUES {values[:-1]}),
        to_update AS (
            SELECT uuid::uuid AS uuid, toestand FROM s)
        UPDATE assets 
        SET toestand = to_update.toestand
        FROM to_update 
        WHERE to_update.uuid = assets.uuid;"""

        with connection.cursor() as cursor:
            cursor.execute(update_query)
