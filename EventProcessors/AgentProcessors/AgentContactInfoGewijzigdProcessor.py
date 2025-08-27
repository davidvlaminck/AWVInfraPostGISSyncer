import json
import logging
import time
from typing import Iterator

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Helpers import chunked, peek_generator


class AgentContactInfoGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started changing contact info of agents')
        start = time.time()

        agent_count = 0
        for uuids_chunk in chunked(uuids, 100):
            generator = self.eminfra_importer.import_resource_from_webservice_by_uuids(uuids=uuids_chunk, resource='agents')

            agent_count += self.update_contact_info(object_generator=generator, connection=connection)

        end = time.time()
        logging.info(f'changed contact info of {agent_count} agents in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def update_contact_info(object_generator: Iterator[dict], connection) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values = ''
        counter = 0
        for agent_dict in object_generator:
            if agent_dict is None:
                continue
            counter += 1
            agent_uuid = agent_dict['@id'].split('/')[-1][0:36]
            contact_info_value = 'NULL'
            if 'purl:Agent.contactinfo' in agent_dict:
                contact_info = agent_dict['purl:Agent.contactinfo']
                contact_info_value = "'" + json.dumps(contact_info).replace("'", "''") + "'"

            values += f"('{agent_uuid}',{contact_info_value}),"

        if counter == 0:
            return 0

        update_query = f"""
        WITH s (uuid, contact_info) 
            AS (VALUES {values[:-1]}),
        t AS (
            SELECT uuid::uuid AS uuid, contact_info::json AS contact_info
            FROM s),
        to_update AS (
            SELECT t.* 
            FROM t
                LEFT JOIN public.agents AS agents ON agents.uuid = t.uuid 
            WHERE agents.uuid IS NOT NULL)
        UPDATE agents 
        SET contact_info = to_update.contact_info
        FROM to_update 
        WHERE to_update.uuid = agents.uuid;"""

        with connection.cursor() as cursor:
            cursor.execute(update_query)

        return counter
