import logging
import time
from typing import Iterator

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Helpers import chunked, peek_generator


class BetrokkenerelatieGeldigheidGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info('started changing geldigheid of betrokkenerelaties')
        start = time.time()

        betrokkenerelatie_count = 0
        for uuids_chunk in chunked(uuids, 100):
            generator = self.eminfra_importer.import_resource_from_webservice_by_uuids(uuids=uuids_chunk, resource='betrokkenerelaties')

            betrokkenerelatie_count += self.update_geldigheid(object_generator=generator, connection=connection)

        end = time.time()
        logging.info(f'changed geldigheid of {betrokkenerelatie_count} betrokkenerelaties in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def update_geldigheid(object_generator: Iterator[dict], connection) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values = ''
        counter = 0
        for betrokkenerelatie_dict in object_generator:
            if betrokkenerelatie_dict is None:
                continue
            counter += 1
            betrokkenerelatie_uuid = betrokkenerelatie_dict['@id'].split('/')[-1][0:36]
            values += f"('{betrokkenerelatie_uuid}',"

            start_datum = betrokkenerelatie_dict.get('HeeftBetrokkene.datumAanvang', None)
            eind_datum = betrokkenerelatie_dict.get('HeeftBetrokkene.datumEinde', None)

            if start_datum is None:
                values += 'NULL,'
            else:
                values += f"'{start_datum}',"

            if eind_datum is None:
                values += 'NULL'
            else:
                values += f"'{eind_datum}'"

        update_query = f"""
        WITH s (uuid, startDatum, eindDatum) 
            AS (VALUES {values[:-1]}),
        t AS (
            SELECT uuid::uuid AS uuid, startDatum::TIMESTAMP as startDatum, eindDatum::TIMESTAMP as eindDatum
            FROM s),
        to_update AS (
            SELECT t.* 
            FROM t
                LEFT JOIN public.betrokkenerelaties AS betrokkenerelaties ON betrokkenerelaties.uuid = t.uuid 
            WHERE betrokkenerelaties.uuid IS NOT NULL)
        UPDATE betrokkenerelaties 
        SET startDatum = to_update.startDatum, eindDatum = to_update.eindDatum
        FROM to_update 
        WHERE to_update.uuid = betrokkenerelaties.uuid;"""

        with connection.cursor() as cursor:
            cursor.execute(update_query)

        return counter
