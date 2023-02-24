from typing import Iterator

from Helpers import peek_generator, turn_list_of_lists_into_string


class IdentiteitUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, safe_insert: bool = False):
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return

        values_array = []
        for identiteit_dict in object_generator:
            record_array = [f"'{identiteit_dict['uuid']}'"]

            identiteit_naam = identiteit_dict.get('naam', '').replace("'", "''")
            identiteit_voornaam = identiteit_dict.get('voornaam', '').replace("'", "''")
            identiteit_gebruikersnaam = identiteit_dict.get('gebruikersnaam', '').replace("'", "''")
            identiteit_type = identiteit_dict.get('_type', '')
            identiteit_vo_id = identiteit_dict.get('voId', '')
            identiteit_bron = identiteit_dict.get('bron', '')

            for val in [identiteit_naam, identiteit_voornaam, identiteit_gebruikersnaam, identiteit_type,
                        identiteit_vo_id, identiteit_bron]:
                if val == '':
                    record_array.append(f"NULL")
                else:
                    record_array.append(f"'{val}'")

            record_array.append(f"{identiteit_dict['actief']}")
            record_array.append(f"{identiteit_dict['systeem']}")

            values_array.append(record_array)

        values_string = turn_list_of_lists_into_string(values_array)

        insert_query = f"""
WITH s (uuid, naam, voornaam, gebruikersnaam, typeIdentiteit, voId, bron, actief, systeem) 
    AS (VALUES {values_string}),
t AS (
    SELECT uuid::uuid AS uuid, naam, voornaam, gebruikersnaam, typeIdentiteit, voId, bron, actief, systeem
    FROM s),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.identiteiten AS identiteiten ON identiteiten.uuid = t.uuid 
    WHERE identiteiten.uuid IS NULL)
INSERT INTO public.identiteiten (uuid, naam, voornaam, gebruikersnaam, typeIdentiteit, voId, bron, actief, systeem)
SELECT to_insert.uuid, to_insert.naam, to_insert.voornaam, to_insert.gebruikersnaam, to_insert.typeIdentiteit, 
    to_insert.voId, to_insert.bron, to_insert.actief, to_insert.systeem
FROM to_insert;"""

        update_query = f"""
WITH s (uuid, naam, voornaam, gebruikersnaam, typeIdentiteit, voId, bron, actief, systeem) 
    AS (VALUES {values_string}),
t AS (
    SELECT uuid::uuid AS uuid, naam, voornaam, gebruikersnaam, typeIdentiteit, voId, bron, actief, systeem
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.identiteiten AS identiteiten ON identiteiten.uuid = t.uuid 
    WHERE identiteiten.uuid IS NOT NULL)
UPDATE identiteiten 
SET naam = to_update.naam, voornaam = to_update.voornaam, gebruikersnaam = to_update.gebruikersnaam, 
    typeIdentiteit = to_update.typeIdentiteit, voId = to_update.voId, bron = to_update.bron, actief = to_update.actief, 
    systeem = to_update.systeem
FROM to_update 
WHERE to_update.uuid = identiteiten.uuid;"""

        cursor = connection.cursor()
        cursor.execute(insert_query)

        cursor = connection.cursor()
        cursor.execute(update_query)
