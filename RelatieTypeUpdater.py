from typing import Iterator

from Helpers import peek_generator, turn_list_of_lists_into_string


class RelatieTypeUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, safe_insert: bool = False):
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return

        values_array = []
        for relatietype_dict in object_generator:
            record_array = [f"'{relatietype_dict['uuid']}'",
                            f"'{relatietype_dict['naam']}'"]

            label = relatietype_dict.get('label', '')
            definitie = relatietype_dict.get('definitie', '').replace("'", "''")
            uri = relatietype_dict.get('uri', '')

            nullables_values = [uri, label, definitie]
            for nullable_value in nullables_values:
                if nullable_value != '':
                    record_array.append(f"'{nullable_value}'")
                else:
                    record_array.append("NULL")

            record_array.append(f"{relatietype_dict['actief']}")
            record_array.append(f"{relatietype_dict['gericht']}")

            values_array.append(record_array)

        values_string = turn_list_of_lists_into_string(values_array)

        insert_query = f"""
WITH s (uuid, naam, uri, label, definitie, actief, gericht) 
    AS (VALUES {values_string}),
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
    AS (VALUES {values_string}),
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
