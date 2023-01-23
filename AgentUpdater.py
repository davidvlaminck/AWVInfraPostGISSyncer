import json
from typing import Iterator

from Helpers import peek_generator


class AgentUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, insert_only: bool = False) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values = ''
        counter = 0
        for agent_dict in object_generator:
            counter += 1
            agent_uuid = agent_dict['@id'].split('/')[-1][0:36]
            agent_name = agent_dict['purl:Agent.naam'].replace("'", "''")
            agent_actief = agent_dict['AIMDBStatus.isActief']
            contact_info_value = 'NULL'
            if 'purl:Agent.contactinfo' in agent_dict:
                contact_info = agent_dict['purl:Agent.contactinfo']
                contact_info_value = "'" + json.dumps(contact_info).replace("'", "''") + "'"

            values += f"('{agent_uuid}','{agent_name}',{agent_actief},{contact_info_value}),"

        insert_query = f"""
        WITH s (uuid, naam, actief, contact_info) 
            AS (VALUES {values[:-1]}),
        t AS (
            SELECT uuid::uuid AS uuid, naam, actief, contact_info::json AS contact_info
            FROM s),
        to_insert AS (
            SELECT t.* 
            FROM t
                LEFT JOIN public.agents AS agents ON agents.uuid = t.uuid 
            WHERE agents.uuid IS NULL)
        INSERT INTO public.agents (uuid, naam, actief, contact_info)
        SELECT to_insert.uuid, to_insert.naam, to_insert.actief, to_insert.contact_info 
        FROM to_insert;"""

        update_query = ''
        if not insert_only:
            update_query = f"""
            WITH s (uuid, naam, actief, contact_info) 
                AS (VALUES {values[:-1]}),
            t AS (
                SELECT uuid::uuid AS uuid, naam, actief, contact_info::json AS contact_info
                FROM s),
            to_update AS (
                SELECT t.* 
                FROM t
                    LEFT JOIN public.agents AS agents ON agents.uuid = t.uuid 
                WHERE agents.uuid IS NOT NULL)
            UPDATE agents 
            SET naam = to_update.naam, actief = to_update.actief, contact_info = to_update.contact_info
            FROM to_update 
            WHERE to_update.uuid = agents.uuid;"""

        with connection.cursor() as cursor:
            cursor.execute(insert_query)

        if not insert_only:
            with connection.cursor() as cursor:
                cursor.execute(update_query)

        return counter
