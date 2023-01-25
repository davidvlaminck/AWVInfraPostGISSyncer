import logging
import time

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor


class NaamGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, eminfra_importer):
        super().__init__(cursor, eminfra_importer)

    def process(self, uuids: [str]):
        logging.info(f'started changing naam/naampad/parent')
        start = time.time()

        asset_dicts = self.em_infra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        values = self.create_values_string_from_dicts(assets_dicts=asset_dicts)
        self.perform_update_with_values(cursor=self.cursor, values=values)
        # TODO change parent uuid as well

        end = time.time()
        logging.info(f'updated {len(asset_dicts)} assets in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def create_values_string_from_dicts(assets_dicts):
        values = ''
        for asset_dict in assets_dicts:
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]

            naam = None
            if 'AIMNaamObject.naam' in asset_dict:
                naam = asset_dict['AIMNaamObject.naam']
            elif 'AbstracteAanvullendeGeometrie.naam' in asset_dict:
                naam = asset_dict['AIMNaamObject.naam']

            naampad = None
            if 'NaampadObject.naampad' in asset_dict:
                naampad = asset_dict['NaampadObject.naampad']

            values += f"('{uuid}',"

            if naam is None:
                values += 'NULL'
            else:
                naam = naam.replace("'", "''")
                values += f"'{naam}'"

            if naampad is None:
                values += ',NULL'
            else:
                naampad = naampad.replace("'", "''")
                values += f",'{naampad}'"
            values = values + '),'
        return values

    @staticmethod
    def perform_update_with_values(cursor, values):
        update_query = f"""
        WITH s (uuid, naam, naampad)  
            AS (VALUES {values[:-1]}),
        to_update AS (
            SELECT uuid::uuid AS uuid, naam, naampad FROM s)
        UPDATE assets 
        SET naam = to_update.naam, naampad = to_update.naampad
        FROM to_update 
        WHERE to_update.uuid = assets.uuid;"""
        cursor.execute(update_query)
