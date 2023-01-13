import json
from typing import Iterator

from EMInfraImporter import EMInfraImporter
from FastFiller import FastFiller
from Helpers import peek_generator
from PostGISConnector import PostGISConnector


class AgentUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection):
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return

        values = ''
        for agent_dict in object_generator:
            agent_uuid = agent_dict['@id'].split('/')[-1][0:36]
            agent_name = agent_dict['purl:Agent.naam'].replace("'", "''")
            contact_info_value = 'NULL'
            if 'purl:Agent.contactinfo' in agent_dict:
                contact_info = agent_dict['purl:Agent.contactinfo']
                contact_info_value = "'" + json.dumps(contact_info).replace("'", "''") + "'"

            values += f"('{agent_uuid}','{agent_name}',{contact_info_value}),"

        insert_query = f"""
    WITH s (uuid, naam, contact_info) 
        AS (VALUES {values[:-1]}),
    t AS (
        SELECT uuid::uuid AS uuid, naam, contact_info::json AS contact_info
        FROM s),
    to_insert AS (
        SELECT t.* 
        FROM t
            LEFT JOIN public.agents AS agents ON agents.uuid = t.uuid 
        WHERE agents.uuid IS NULL)
    INSERT INTO public.agents (uuid, naam, contact_info, actief)
    SELECT to_insert.uuid, to_insert.naam, to_insert.contact_info, true 
    FROM to_insert;"""

        update_query = f"""
    WITH s (uuid, naam, contact_info) 
        AS (VALUES {values[:-1]}),
    t AS (
        SELECT uuid::uuid AS uuid, naam, contact_info::json AS contact_info
        FROM s),
    to_update AS (
        SELECT t.* 
        FROM t
            LEFT JOIN public.agents AS agents ON agents.uuid = t.uuid 
        WHERE agents.uuid IS NOT NULL)
    UPDATE agents 
    SET naam = to_update.naam, contact_info = to_update.contact_info
    FROM to_update 
    WHERE to_update.uuid = agents.uuid;"""

        cursor = connection.cursor()
        cursor.execute(insert_query)

        cursor = connection.cursor()
        cursor.execute(update_query)
        connection.commit()


class AgentSyncer(FastFiller):
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter, resource: str):
        super().__init__(resource=resource, postgis_connector=postgis_connector, eminfra_importer=eminfra_importer)
        self.updater = AgentUpdater()

