from datetime import datetime
from typing import Iterator

from Helpers import peek_generator, turn_list_of_lists_into_string


class AanleidingUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, safe_insert: bool = False):
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return

        values_array = []
        for aanleiding_dict in object_generator:
            record_array = [f"'{aanleiding_dict['@id'].split('/')[-1][:36]}'",
                            f"'{aanleiding_dict['@type']}'",
                            'TRUE']

            values_array.append(record_array)

        values_string = turn_list_of_lists_into_string(values_array)

        insert_query = f"""
        WITH s (uuid, assetTypeUri, actief) 
            AS (VALUES {values_string}),
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

        update_query = f"""
        WITH s (uuid, assetTypeUri, actief)  
            AS (VALUES {values_string}),
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

        cursor = connection.cursor()
        cursor.execute(insert_query)

        cursor = connection.cursor()
        cursor.execute(update_query)
