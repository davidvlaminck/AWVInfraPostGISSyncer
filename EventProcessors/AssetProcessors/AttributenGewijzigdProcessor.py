import logging
import time
from typing import Dict
import re

import psycopg2

from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Exceptions.AssetMissingError import AssetMissingError
from Exceptions.AttribuutMissingError import AttribuutMissingError
from Helpers import turn_list_of_lists_into_string
from ResourceEnum import colorama_table, ResourceEnum


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
        values_string, amount = AttributenGewijzigdProcessor.create_values_string_from_dicts(assets_dicts=asset_dicts)
        if values_string != '':
            AttributenGewijzigdProcessor.perform_update_with_values(connection=connection, values_string=values_string)
        return amount

    @staticmethod
    def remove_existing_attributes(asset_uuids: [str], connection):
        if len(asset_uuids) == 0:
            return
        delete_query = "DELETE FROM public.attribuutWaarden WHERE assetUuid IN (VALUES ('" + "'::uuid),('".join(
            asset_uuids) + "'::uuid));"

        with connection.cursor() as cursor:
            cursor.execute(delete_query)

    @staticmethod
    def create_values_string_from_dicts(assets_dicts: [Dict]):
        counter = 0
        values_array = []
        for asset_dict in assets_dicts:
            counter += 1
            asset_uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]
            for key, value in asset_dict.items():
                if key in ['@type', '@id', 'NaampadObject.naampad', 'AIMObject.notitie', 'AIMObject.typeURI',
                           'AIMDBStatus.isActief', 'AIMNaamObject.naam', 'AIMToestand.toestand', 'geometry',
                           'bs:Bestek.bestekkoppeling']:
                    continue
                if key.startswith('tz:') or key.startswith('geo:') or key.startswith('loc:') or key.startswith('wl:'):
                    continue
                if key.startswith('lgc:') or key.startswith('ond:') or key.startswith('ins:') or key.startswith(
                        'grp:') or key.startswith('vtc:'):
                    key = key[4:]
                elif key.startswith('bz:'):
                    key = key[3:]
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
                values_array.append([f"'{asset_uuid}'", f"'{key}'", f"'{value}'"])

        values_string = turn_list_of_lists_into_string(values_array)

        return values_string, counter

    @staticmethod
    def perform_update_with_values(connection, values_string: str):
        insert_query = f"""
WITH s (assetUuid, attribute_name, waarde) 
    AS (VALUES {values_string}),
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
            msg = str(exc).split('\n')[0]
            if 'null value in column "attribuutuuid"' in msg and 'violates not-null constraint' in msg:
                logging.error('raising AttribuutMissingError: ' + str(exc).split('\n')[1])
                raise AttribuutMissingError()
            elif 'violates foreign key constraint "assets_attribuutwaarden_fkey"' in msg:
                exception_to_raise = AssetMissingError()
                if '\n' in str(exc):
                    detail_line = str(exc).split('\n')[1]
                    exception_to_raise.asset_uuids = re.findall(r"assetuuid\)=\((.*?)\)", detail_line)
                    logging.error(f'{colorama_table[ResourceEnum.assets]}{detail_line}')
                connection.rollback()
                logging.error(f'{colorama_table[ResourceEnum.assets]}raising AssetMissingError')
                raise exception_to_raise
            else:
                raise exc
