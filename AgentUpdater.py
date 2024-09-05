import json
from typing import Iterator

from Helpers import peek_generator, turn_list_of_lists_into_string


class AgentUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, insert_only: bool = False,
                       safe_insert: bool = False) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values_array = []

        counter = 0
        for agent_dict in object_generator:
            counter += 1
            record_array = [f"'{agent_dict['@id'].split('/')[-1][0:36]}'"]

            agent_name = agent_dict['purl:Agent.naam'].replace("'", "''")
            record_array.append(f"'{agent_name}'")

            record_array.append(f"{agent_dict.get('AIMDBStatus.isActief', True)}")

            contact_info_value = 'NULL'
            if 'purl:Agent.contactinfo' in agent_dict:
                contact_info = agent_dict['purl:Agent.contactinfo']
                contact_info_value = "'" + json.dumps(contact_info).replace("'", "''") + "'"

            record_array.append(f"{contact_info_value}")

            ovo_code = agent_dict.get('tz:Agent.ovoCode', 'NULL')
            if ovo_code != 'NULL':
                ovo_code = "'" + ovo_code + "'"
            record_array.append(f"{ovo_code}")

            values_array.append(record_array)

        if len(values_array) == 0:
            return 0

        values_string = turn_list_of_lists_into_string(values_array)

        insert_query = f"""
        WITH s (uuid, naam, actief, contact_info, ovo_code) 
            AS (VALUES {values_string}),
        t AS (
            SELECT uuid::uuid AS uuid, naam, actief, contact_info::json AS contact_info, ovo_code
            FROM s),
        to_insert AS (
            SELECT t.* 
            FROM t
                LEFT JOIN public.agents AS agents ON agents.uuid = t.uuid 
            WHERE agents.uuid IS NULL)
        INSERT INTO public.agents (uuid, naam, actief, contact_info, ovo_code)
        SELECT to_insert.uuid, to_insert.naam, to_insert.actief, to_insert.contact_info, to_insert.ovo_code 
        FROM to_insert;"""

        update_query = ''
        if not insert_only:
            update_query = f"""
            WITH s (uuid, naam, actief, contact_info, ovo_code) 
                AS (VALUES {values_string}),
            t AS (
                SELECT uuid::uuid AS uuid, naam, actief, contact_info::json AS contact_info, ovo_code
                FROM s),
            to_update AS (
                SELECT t.* 
                FROM t
                    LEFT JOIN public.agents AS agents ON agents.uuid = t.uuid 
                WHERE agents.uuid IS NOT NULL)
            UPDATE agents 
            SET naam = to_update.naam, actief = to_update.actief, contact_info = to_update.contact_info, 
                ovo_code = to_update.ovo_code
            FROM to_update 
            WHERE to_update.uuid = agents.uuid;"""

        with connection.cursor() as cursor:
            cursor.execute(insert_query)

        if not insert_only:
            with connection.cursor() as cursor:
                cursor.execute(update_query)

        return counter
