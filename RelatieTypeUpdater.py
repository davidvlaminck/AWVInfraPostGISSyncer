from typing import Iterator

from Helpers import peek_generator


class RelatieTypeUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, safe_insert: bool = False):
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return

        values = ''
        for relatietype_dict in object_generator:
            uuid = relatietype_dict['uuid']
            naam = relatietype_dict['naam']
            label = relatietype_dict.get('label', '')
            definitie = relatietype_dict.get('definitie', '').replace("'", "''")
            actief = relatietype_dict['actief']
            gericht = relatietype_dict['gericht']
            uri = relatietype_dict.get('uri', '')

            values += f"('{uuid}','{naam}',"
            null_values = [uri, label, definitie]
            for null_value in null_values:
                if null_value != '':
                    values += f"'{null_value}',"
                else:
                    values += "NULL,"
            values += f"{actief},{gericht}),"

        insert_query = f"""
WITH s (uuid, naam, uri, label, definitie, actief, gericht) 
    AS (VALUES {values[:-1]}),
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

        update_query = f"""
WITH s (uuid, naam, uri, label, definitie, actief, gericht)
    AS (VALUES {values[:-1]}),
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

        cursor = connection.cursor()
        cursor.execute(insert_query)

        cursor = connection.cursor()
        cursor.execute(update_query)
