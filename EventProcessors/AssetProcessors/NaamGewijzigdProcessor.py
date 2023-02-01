import logging
import time

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor


class NaamGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started changing naam/naampad/parent')
        start = time.time()

        asset_dicts = self.eminfra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        values, amount = self.create_values_string_from_dicts(assets_dicts=asset_dicts)
        self.perform_update_with_values(connection=connection, values=values)
        # TODO change parent uuid as well

        end = time.time()
        logging.info(f'updated naam/naampad/parent of {amount} asset(s) in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def create_values_string_from_dicts(assets_dicts):
        values = ''
        counter = 0
        for asset_dict in assets_dicts:
            counter += 1
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
        return values, counter

    @staticmethod
    def perform_update_with_values(connection, values):
        update_query = f"""
        WITH s (uuid, naam, naampad)  
            AS (VALUES {values[:-1]}),
        to_update AS (
            SELECT uuid::uuid AS uuid, naam, naampad FROM s)
        UPDATE assets 
        SET naam = to_update.naam, naampad = to_update.naampad
        FROM to_update 
        WHERE to_update.uuid = assets.uuid;"""

        with connection.cursor() as cursor:
            cursor.execute(update_query)
