import logging
import time
from typing import Iterator

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Helpers import chunked, peek_generator


class AgentOvocodeGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started changing ovo_code of agents')
        start = time.time()

        agent_count = 0
        for uuids in chunked(uuids, 100):
            generator = self.eminfra_importer.import_resource_from_webservice_by_uuids(uuids=uuids, resource='agents')

            agent_count += self.update_name(object_generator=generator, connection=connection)

        end = time.time()
        logging.info(f'changed ovo_code of {agent_count} agents in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def update_name(object_generator: Iterator[dict], connection) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values = ''
        counter = 0
        for agent_dict in object_generator:
            counter += 1
            agent_uuid = agent_dict['@id'].split('/')[-1][0:36]
            if 'tz:Agent.ovoCode' not in agent_dict:
                agent_ovo_code = 'NULL'
            else:
                agent_ovo_code = "'" + agent_dict['tz:Agent.ovoCode'].replace("'", "''") + "'"

            values += f"('{agent_uuid}',{agent_ovo_code}),"

        update_query = f"""
        WITH s (uuid, ovo_code) 
            AS (VALUES {values[:-1]}),
        t AS (
            SELECT uuid::uuid AS uuid, ovo_code
            FROM s),
        to_update AS (
            SELECT t.* 
            FROM t
                LEFT JOIN public.agents AS agents ON agents.uuid = t.uuid 
            WHERE agents.uuid IS NOT NULL)
        UPDATE agents 
        SET ovo_code = to_update.ovo_code
        FROM to_update 
        WHERE to_update.uuid = agents.uuid;"""

        with connection.cursor() as cursor:
            cursor.execute(update_query)

        return counter
