from datetime import datetime
from typing import Iterator

from Helpers import peek_generator


class BeheerderUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, safe_insert: bool = False):
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return

        values_array = []
        for beheerder_dict in object_generator:
            record_array = [beheerder_dict['uuid']]

            beheerder_naam = beheerder_dict.get('naam', '')
            beheerder_referentie = beheerder_dict.get('referentie', '')
            beheerder_type = beheerder_dict.get('_type', '')
            beheerder_actief = True
            actief_interval = beheerder_dict['actiefInterval']
            if 'van' not in actief_interval:
                beheerder_actief = False
            else:
                van_date = datetime.strptime(actief_interval['van'], '%Y-%m-%d')
                if van_date > datetime.now():
                    beheerder_actief = False
                else:
                    if 'tot' in actief_interval:
                        tot_date = datetime.strptime(actief_interval['tot'], '%Y-%m-%d')
                        if tot_date < datetime.now():
                            beheerder_actief = False

            for val in [beheerder_naam, beheerder_referentie, beheerder_type]:
                record_array.append(val if val != '' else None)

            record_array.append(beheerder_actief)
            values_array.append(record_array)

        insert_query = """
    WITH s (uuid, naam, referentie, typeBeheerder, actief) 
        AS (VALUES %s),
    t AS (
        SELECT uuid::uuid AS uuid, naam, referentie, typeBeheerder, actief
        FROM s),
    to_insert AS (
        SELECT t.* 
        FROM t
            LEFT JOIN public.beheerders AS beheerders ON beheerders.uuid = t.uuid 
        WHERE beheerders.uuid IS NULL)
    INSERT INTO public.beheerders (uuid, naam, referentie, typeBeheerder, actief)
    SELECT to_insert.uuid, to_insert.naam, to_insert.referentie, to_insert.typeBeheerder, to_insert.actief
    FROM to_insert;"""

        update_query = """
    WITH s (uuid, naam, referentie, typeBeheerder, actief) 
        AS (VALUES %s),
    t AS (
        SELECT uuid::uuid AS uuid, naam, referentie, typeBeheerder, actief
        FROM s),
    to_update AS (
        SELECT t.* 
        FROM t
            LEFT JOIN public.beheerders AS beheerders ON beheerders.uuid = t.uuid 
        WHERE beheerders.uuid IS NOT NULL)
    UPDATE beheerders 
    SET naam = to_update.naam, referentie = to_update.referentie, typeBeheerder = to_update.typeBeheerder, 
        actief = to_update.actief
    FROM to_update 
    WHERE to_update.uuid = beheerders.uuid;"""

        from psycopg2.extras import execute_values
        cursor = connection.cursor()
        execute_values(cursor, insert_query, values_array)

        cursor = connection.cursor()
        execute_values(cursor, update_query, values_array)
