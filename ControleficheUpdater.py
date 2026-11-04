import logging
from typing import Iterator

import psycopg2

from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.AttributenGewijzigdProcessor import AttributenGewijzigdProcessor
from Exceptions.AssetTypeMissingError import AssetTypeMissingError
from Helpers import peek_generator


class ControleficheUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection,
                       insert_only: bool = False, safe_insert: bool = False) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        counter = 0
        controlefiche_dict = {}
        counter, values_array = ControleficheUpdater.fill_values_from_object_generator(
            controlefiche_dict=controlefiche_dict, counter=counter, object_generator=object_generator)

        if not controlefiche_dict:
            return 0

        ControleficheUpdater.perform_insert_update_from_values(connection, insert_only, values_array)

        AttributenGewijzigdProcessor.process_dicts(connection=connection, asset_uuids=list(controlefiche_dict.keys()),
                                                   asset_dicts=list(controlefiche_dict.values()))

        logging.info(f'Updated or inserted {counter} controlefiche objects')
        return counter

    @staticmethod
    def fill_values_from_object_generator(controlefiche_dict: dict, counter: int, object_generator: Iterator[dict]):
        values_array = []
        for cf_dict in object_generator:
            if cf_dict['@id'].startswith('https://data.awvvlaanderen.be/id/assetrelatie/'):
                continue

            if not cf_dict['@type'].startswith('https://bz.'):
                continue

            controlefiche_uuid = cf_dict['@id'].split('/')[-1][:36]

            if controlefiche_uuid in controlefiche_dict:
                continue

            counter += 1
            controlefiche_dict[controlefiche_uuid] = cf_dict
            record_array = [controlefiche_uuid, cf_dict['@type'], True]

            values_array.append(record_array)

        return counter, values_array

    @staticmethod
    def perform_insert_update_from_values(connection, insert_only, values_array):
        from psycopg2.extras import execute_values
        insert_query = """
WITH s (uuid, assetTypeUri, actief) 
    AS (VALUES %s),
t AS (
    SELECT s.uuid::uuid AS uuid, assettypes.uuid as assettype, s.actief
    FROM s
        LEFT JOIN assettypes ON assettypes.uri = s.assetTypeUri),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.assets AS assets ON assets.uuid = t.uuid 
    WHERE assets.uuid IS NULL)
INSERT INTO public.assets (uuid, assettype, actief) 
SELECT to_insert.uuid, to_insert.assettype, to_insert.actief
FROM to_insert;"""
        update_query = ''
        if not insert_only:
            update_query = """
WITH s (uuid, assetTypeUri, actief)  
    AS (VALUES %s),
t AS (
    SELECT s.uuid::uuid AS uuid, assettypes.uuid as assettype, s.actief
    FROM s
        LEFT JOIN assettypes ON assettypes.uri = s.assetTypeUri),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.assets AS assets ON assets.uuid = t.uuid 
    WHERE assets.uuid IS NOT NULL)
UPDATE assets 
SET actief = to_update.actief
FROM to_update 
WHERE to_update.uuid = assets.uuid;"""

        try:
            with connection.cursor() as cursor:
                execute_values(cursor, insert_query, values_array)
                if not insert_only:
                    execute_values(cursor, update_query, values_array)
        except psycopg2.errors.NotNullViolation as exc:
            first_line = exc.args[0].split('\n')[0]
            if 'null value in column "assettype"' in first_line and 'violates not-null constraint' in first_line:
                if '\n' in str(exc):
                    logging.error(str(exc).split('\n')[1])
                connection.rollback()
                logging.error('raising AssetTypeMissingError')
                raise AssetTypeMissingError() from exc
        except psycopg2.Error as exc:
            logging.error(exc)
            connection.rollback()
            raise exc
