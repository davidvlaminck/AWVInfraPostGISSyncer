from datetime import datetime
from typing import Iterator

from Helpers import peek_generator


class ToezichtgroepUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection):
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return

        values = ''
        for toezichtgroep_dict in object_generator:
            toezichtgroep_uuid = toezichtgroep_dict['uuid']
            toezichtgroep_naam = toezichtgroep_dict['naam'].replace("'", "''")
            toezichtgroep_ref = toezichtgroep_dict['referentie'].replace("'", "''")
            toezichtgroep_type = toezichtgroep_dict['_type']
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
                    else:
                        if 'tot' in actiefInterval:
                            tot_date = datetime.strptime(actiefInterval['tot'], '%Y-%m-%d')
                            if tot_date < datetime.now():
                                toezichtgroep_actief = False

            values += f"('{toezichtgroep_uuid}','{toezichtgroep_naam}','{toezichtgroep_ref}','{toezichtgroep_type}',{toezichtgroep_actief}),"

        insert_query = f"""
WITH s (uuid, naam, referentie, typeGroep, actief) 
    AS (VALUES {values[:-1]}),
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

        update_query = f"""
WITH s (uuid, naam, referentie, typeGroep, actief) 
    AS (VALUES {values[:-1]}),
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

        cursor = connection.cursor()
        cursor.execute(insert_query)

        cursor = connection.cursor()
        cursor.execute(update_query)