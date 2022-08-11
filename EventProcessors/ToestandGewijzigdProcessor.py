import logging
import time

from EventProcessors.SpecificEventProcessor import SpecificEventProcessor


class ToestandGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, em_infra_importer):
        super().__init__(cursor, em_infra_importer)

    def process(self, uuids: [str]):
        logging.info(f'started updating toestand')
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
        return values

    @staticmethod
    def perform_update_with_values(cursor, values):
        update_query = f"""
        WITH s (uuid, toestand)  
            AS (VALUES {values[:-1]}),
        to_update AS (
            SELECT uuid::uuid AS uuid, toestand FROM s)
        UPDATE assets 
        SET toestand = to_update.toestand
        FROM to_update 
        WHERE to_update.uuid = assets.uuid;"""
        cursor.execute(update_query)
