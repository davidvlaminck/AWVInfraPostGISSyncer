from datetime import datetime
from typing import Iterator

from Helpers import peek_generator


class ToezichtgroepUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, safe_insert: bool = False):
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return

        values_array = []
        for toezichtgroep_dict in object_generator:
            record_array = [toezichtgroep_dict['uuid']]

            record_array.append(toezichtgroep_dict['naam'])
            record_array.extend((toezichtgroep_dict['referentie'], toezichtgroep_dict['_type']))
            toezichtgroep_actief = True
            if 'actiefInterval' not in toezichtgroep_dict:
                toezichtgroep_actief = False
            else:
                actiefInterval = toezichtgroep_dict['actiefInterval']
                if 'van' not in actiefInterval:
                    toezichtgroep_actief = False
                else:
                    van_date = datetime.strptime(actiefInterval['van'], '%Y-%m-%d')
                    if van_date > datetime.now():
                        toezichtgroep_actief = False
                    elif 'tot' in actiefInterval:
                        tot_date = datetime.strptime(actiefInterval['tot'], '%Y-%m-%d')
                        if tot_date < datetime.now():
                            toezichtgroep_actief = False

            record_array.append(toezichtgroep_actief)

            values_array.append(record_array)

        insert_query = """
WITH s (uuid, naam, referentie, typeGroep, actief) 
    AS (VALUES %s),
t AS (
    SELECT uuid::uuid AS uuid, naam, referentie, typeGroep, actief
    FROM s),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.toezichtgroepen AS toezichtgroepen ON toezichtgroepen.uuid = t.uuid 
    WHERE toezichtgroepen.uuid IS NULL)
INSERT INTO public.toezichtgroepen (uuid, naam, referentie, typeGroep, actief)
SELECT to_insert.uuid, to_insert.naam, to_insert.referentie, to_insert.typeGroep, to_insert.actief
FROM to_insert;"""

        update_query = """
WITH s (uuid, naam, referentie, typeGroep, actief) 
    AS (VALUES %s),
t AS (
    SELECT uuid::uuid AS uuid, naam, referentie, typeGroep, actief
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.toezichtgroepen AS toezichtgroepen ON toezichtgroepen.uuid = t.uuid 
    WHERE toezichtgroepen.uuid IS NOT NULL)
UPDATE toezichtgroepen 
SET naam = to_update.naam, referentie = to_update.referentie, typeGroep = to_update.typeGroep, actief = to_update.actief
FROM to_update 
WHERE to_update.uuid = toezichtgroepen.uuid;"""

        from psycopg2.extras import execute_values
        cursor = connection.cursor()
        execute_values(cursor, insert_query, values_array)

        cursor = connection.cursor()
        execute_values(cursor, update_query, values_array)
