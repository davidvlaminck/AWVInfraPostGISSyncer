import json

from EMInfraImporter import EMInfraImporter
from EventProcessors.BetrokkeneRelatiesGewijzigdProcessor import BetrokkeneRelatiesGewijzigdProcessor
from PostGISConnector import PostGISConnector


class BetrokkeneRelatiesSyncer:
    def __init__(self, post_gis_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postGIS_connector = post_gis_connector
        self.eminfra_importer = em_infra_importer

    def sync_betrokkenerelaties(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            cursor = self.postGIS_connector.connection.cursor()
            processor = BetrokkeneRelatiesGewijzigdProcessor(cursor=cursor, em_infra_importer=self.eminfra_importer,
                                                             connector=self.postGIS_connector)
            relaties = self.eminfra_importer.import_betrokkenerelaties_from_webservice_page_by_page(page_size=page_size)
            if len(relaties) == 0:
                break

            asset_uuids = list(set(map(lambda x: x['bron']['uuid'], relaties)))
            processor.process_dicts(betrokkenerelatie_dicts=relaties, cursor=cursor, asset_uuids=asset_uuids)
            self.postGIS_connector.save_props_to_params(cursor=cursor, params={'pagingcursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break

    def update_agents(self, agent_dicts: [dict]):
        if len(agent_dicts) == 0:
            return

        values = ''
        for agent_dict in agent_dicts:
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

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(insert_query)

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(update_query)
        self.postGIS_connector.connection.commit()
