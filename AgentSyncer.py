import json

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class AgentSyncer:
    def __init__(self, postGIS_connector: PostGISConnector, emInfraImporter: EMInfraImporter):
        self.postGIS_connector = postGIS_connector
        self.emInfraImporter = emInfraImporter

    def sync_agents(self):
        self.update_all_agents()
        self.postGIS_connector.connection.commit()

    def update_all_agents(self):
        agents = self.get_all_agents()
        self.update_agents(agent_dicts=agents)

    def get_all_agents(self) -> []:
        return self.emInfraImporter.import_all_agents_from_webservice()

    def update_agents(self, agent_dicts: [dict]):
        if len(agent_dicts) == 0:
            return

        values = ''
        for agent_dict in agent_dicts:
            agent_uuid = agent_dict['@id'].split('/')[-1][0:36]
            agent_name = agent_dict['purl:Agent.naam'].replace("'", "''")
            contact_info = {}
            if 'purl:Agent.contactinfo' in agent_dict:
                contact_info = agent_dict['purl:Agent.contactinfo']
            values += f"('{agent_uuid}','{agent_name}','{json.dumps(contact_info)}'),"

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

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(insert_query)

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(update_query)
        self.postGIS_connector.connection.commit()
