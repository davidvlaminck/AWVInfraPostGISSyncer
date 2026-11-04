import json
from typing import Iterator

from Helpers import peek_generator


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
            record_array = [agent_dict['@id'].split('/')[-1][0:36]]

            agent_name = agent_dict['purl:Agent.naam']
            record_array.append(agent_name)

            record_array.append(agent_dict.get('AIMDBStatus.isActief', True))

            contact_info_value = None
            if 'purl:Agent.contactinfo' in agent_dict:
                contact_info = agent_dict['purl:Agent.contactinfo']
                contact_info_value = json.dumps(contact_info)

            record_array.append(contact_info_value)

            ovo_code = agent_dict.get('tz:Agent.ovoCode', None)
            record_array.append(ovo_code)

            values_array.append(record_array)

        if len(values_array) == 0:
            return 0

        insert_query = """
        WITH s (uuid, naam, actief, contact_info, ovo_code) 
            AS (VALUES %s),
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
            update_query = """
            WITH s (uuid, naam, actief, contact_info, ovo_code) 
                AS (VALUES %s),
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

        from psycopg2.extras import execute_values
        with connection.cursor() as cursor:
            execute_values(cursor, insert_query, values_array)

        if not insert_only:
            with connection.cursor() as cursor:
                execute_values(cursor, update_query, values_array)

        return counter
