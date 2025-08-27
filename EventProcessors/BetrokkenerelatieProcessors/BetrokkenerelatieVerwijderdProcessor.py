import logging
import time
from typing import Iterator

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Helpers import chunked, peek_generator


class BetrokkenerelatieVerwijderdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info('started removing betrokkenerelaties')
        start = time.time()

        betrokkenerelatie_count = 0
        for uuids_chunk in chunked(uuids, 100):
            betrokkenerelatie_count += self.update_actief(object_generator=iter(uuids_chunk), connection=connection)

        end = time.time()
        logging.info(f'removed {betrokkenerelatie_count} betrokkenerelaties in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def update_actief(object_generator: Iterator[dict], connection) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values = ''
        counter = 0
        for betrokkenerelatie_uuid in object_generator:
            if betrokkenerelatie_uuid is None:
                continue
            counter += 1
            betrokkenerelatie_actief = False

            values += f"('{betrokkenerelatie_uuid}',{betrokkenerelatie_actief}),"

        if counter == 0:
            return 0

        update_query = f"""
        WITH s (uuid, actief) 
            AS (VALUES {values[:-1]}),
        t AS (
            SELECT uuid::uuid AS uuid, actief
            FROM s),
        to_update AS (
            SELECT t.* 
            FROM t
                LEFT JOIN public.betrokkenerelaties AS betrokkenerelaties ON betrokkenerelaties.uuid = t.uuid 
            WHERE betrokkenerelaties.uuid IS NOT NULL)
        UPDATE betrokkenerelaties 
        SET actief = to_update.actief
        FROM to_update 
        WHERE to_update.uuid = betrokkenerelaties.uuid;"""

        with connection.cursor() as cursor:
            cursor.execute(update_query)

        return counter
