import logging
import time
from typing import Iterator

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Helpers import chunked, peek_generator


class AgentVOIDGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started changing VO ID agents')
        start = time.time()

        agent_count = 0
        for uuids in chunked(uuids, 100):
            generator = self.eminfra_importer.import_resource_from_webservice_by_uuids(uuids=uuids, resource='agents',
                                                                                       oslo_endpoint=False)

            agent_count += self.update_vo_id(object_generator=generator, connection=connection)

        end = time.time()
        logging.info(f'changed VO ID of {agent_count} agents in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def update_vo_id(object_generator: Iterator[dict], connection) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values = ''
        counter = 0
        for agent_dict in object_generator:
            counter += 1
            agent_uuid = agent_dict['uuid']
            agent_vo_id = agent_dict['voId']

            values += f"('{agent_uuid}','{agent_vo_id}'),"

        update_query = f"""
        WITH s (uuid, void) 
            AS (VALUES {values[:-1]}),
        t AS (
            SELECT uuid::uuid AS uuid, void::uuid as void
            FROM s),
        to_update AS (
            SELECT t.* 
            FROM t
                LEFT JOIN public.agents AS agents ON agents.uuid = t.uuid 
            WHERE agents.uuid IS NOT NULL)
        UPDATE agents 
        SET void = to_update.void
        FROM to_update 
        WHERE to_update.uuid = agents.uuid;"""

        with connection.cursor() as cursor:
            cursor.execute(update_query)

        return counter
