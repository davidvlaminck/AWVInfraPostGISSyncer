import logging
import time

import psycopg2

from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Exceptions.AttribuutMissingError import AttribuutMissingError


class AttributenGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer: EMInfraImporter):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started updating attributes')
        start = time.time()

        asset_dicts = self.eminfra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        amount = self.process_dicts(connection=connection, asset_uuids=uuids, asset_dicts=asset_dicts)

        end = time.time()
        logging.info(f'updated attributes of {amount} asset(s) in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def process_dicts(connection, asset_uuids, asset_dicts):
        AttributenGewijzigdProcessor.remove_existing_attributes(connection=connection, asset_uuids=asset_uuids)
        values, amount = AttributenGewijzigdProcessor.create_values_string_from_dicts(assets_dicts=asset_dicts)
        AttributenGewijzigdProcessor.perform_update_with_values(connection=connection, values=values)
        return amount

    @staticmethod
    def remove_existing_attributes(asset_uuids, connection):
        if len(asset_uuids) == 0:
            return
        delete_query = "DELETE FROM public.attribuutWaarden WHERE assetUuid IN (VALUES ('" + "'::uuid),('".join(
            asset_uuids) + "'::uuid));"

        with connection.cursor() as cursor:
            cursor.execute(delete_query)

    @staticmethod
    def create_values_string_from_dicts(assets_dicts):
        values = ''
        counter = 0
        for asset_dict in assets_dicts:
            counter += 1
            asset_uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]
            for key, value in asset_dict.items():
                if key in ['@type', '@id', 'NaampadObject.naampad', 'AIMObject.notitie', 'AIMObject.typeURI',
                           'AIMDBStatus.isActief', 'AIMNaamObject.naam', 'AIMToestand.toestand', 'geometry']:
                    continue
                if key.startswith('tz:') or key.startswith('geo:') or key.startswith('loc:'):
                    continue
                if key.startswith('lgc:') or key.startswith('ond:') or key.startswith('ins:') or key.startswith('grp:'):
                    key = key[4:]
                if isinstance(value, dict):
                    value = str(value)
                elif isinstance(value, list):
                    value_list = ''
                    for item in value:
                        value_list += str(item) + '|'
                    if len(value_list) > 0:
                        value = value_list[:-1]
                    else:
                        value = ''
                if not isinstance(value, str):
                    value = str(value)
                value = value.replace("'", "''")
                values += f"('{asset_uuid}','{key}', '{value}'),\n"

        return values, counter

    @staticmethod
    def perform_update_with_values(connection, values):
        insert_query = f"""
WITH s (assetUuid, attribute_name, waarde) 
    AS (VALUES {values[:-2]}),
to_insert AS (
    SELECT assetUuid::uuid, waarde, attributen.uuid::uuid AS attribuutUuid 
    FROM s 
        LEFT JOIN attributen ON attributen.uri LIKE '%' || '#' || attribute_name)
INSERT INTO public.attribuutWaarden (assetUuid, attribuutUuid, waarde)
SELECT to_insert.assetUuid, to_insert.attribuutUuid, to_insert.waarde
FROM to_insert;"""
        try:
            with connection.cursor() as cursor:
                cursor.execute(insert_query)
        except psycopg2.Error as exc:
            if str(exc).split('\n')[0] == 'null value in column "attribuutuuid" violates not-null constraint':
                raise AttribuutMissingError()
            else:
                raise exc

