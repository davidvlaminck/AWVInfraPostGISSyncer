from typing import Iterator

from Helpers import peek_generator


class RelatieTypeUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, safe_insert: bool = False):
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return

        values_array = []
        for relatietype_dict in object_generator:
            record_array = [relatietype_dict['uuid'], relatietype_dict['naam']]

            label = relatietype_dict.get('label', '')
            definitie = relatietype_dict.get('definitie', '')
            uri = relatietype_dict.get('uri', '')

            nullables_values = [uri, label, definitie]
            for nullable_value in nullables_values:
                record_array.append(nullable_value if nullable_value != '' else None)

            record_array.append(relatietype_dict['actief'])
            record_array.append(relatietype_dict['gericht'])

            values_array.append(record_array)

        insert_query = """
WITH s (uuid, naam, uri, label, definitie, actief, gericht) 
    AS (VALUES %s),
t AS (
    SELECT uuid::uuid AS uuid, naam, uri, label, definitie, actief, gericht
    FROM s),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.relatietypes ON relatietypes.uuid = t.uuid 
    WHERE relatietypes.uuid IS NULL)
INSERT INTO public.relatietypes (uuid, naam, uri, label, definitie, actief, gericht)
SELECT to_insert.uuid, to_insert.naam, to_insert.uri, to_insert.label, to_insert.definitie, to_insert.actief, to_insert.gericht
FROM to_insert;"""

        update_query = """
WITH s (uuid, naam, uri, label, definitie, actief, gericht)
    AS (VALUES %s),
t AS (
    SELECT uuid::uuid AS uuid, naam, uri, label, definitie, actief, gericht
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.relatietypes ON relatietypes.uuid = t.uuid 
    WHERE relatietypes.uuid IS NOT NULL)
UPDATE public.relatietypes 
SET naam = to_update.naam, uri = to_update.uri, label = to_update.label, definitie = to_update.definitie, actief = to_update.actief, 
    gericht = to_update.gericht
FROM to_update 
WHERE to_update.uuid = relatietypes.uuid;"""

        from psycopg2.extras import execute_values
        cursor = connection.cursor()
        execute_values(cursor, insert_query, values_array)

        cursor = connection.cursor()
        execute_values(cursor, update_query, values_array)
