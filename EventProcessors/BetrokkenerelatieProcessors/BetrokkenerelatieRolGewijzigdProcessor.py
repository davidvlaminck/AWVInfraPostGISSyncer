import logging
import time
from typing import Iterator

from EventProcessors.SpecificEventProcessor import SpecificEventProcessor
from Helpers import chunked, peek_generator


class BetrokkenerelatieRolGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started changing rol of betrokkenerelaties')
        start = time.time()

        betrokkenerelatie_count = 0
        for uuids in chunked(uuids, 100):
            generator = self.em_infra_importer.import_resource_from_webservice_by_uuids(uuids=uuids,
                                                                                        resource='betrokkenerelaties')

            betrokkenerelatie_count += self.update_rol(object_generator=generator, connection=connection)

        end = time.time()
        logging.info(f'changed rol of {betrokkenerelatie_count} betrokkenerelaties in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def update_rol(object_generator: Iterator[dict], connection) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values = ''
        counter = 0
        for betrokkenerelatie_dict in object_generator:
            counter += 1
            betrokkenerelatie_uuid = betrokkenerelatie_dict['@id'].split('/')[-1][0:36]
            if 'HeeftBetrokkene.rol' in betrokkenerelatie_dict:
                betrokkenerelatie_rol = "'" + betrokkenerelatie_dict['HeeftBetrokkene.rol'].replace('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlBetrokkenheidRol/', '') + "'"
            else:
                betrokkenerelatie_rol = 'NULL'

            values += f"('{betrokkenerelatie_uuid}',{betrokkenerelatie_rol}),"

        update_query = f"""
        WITH s (uuid, rol) 
            AS (VALUES {values[:-1]}),
        t AS (
            SELECT uuid::uuid AS uuid, rol
            FROM s),
        to_update AS (
            SELECT t.* 
            FROM t
                LEFT JOIN public.betrokkenerelaties AS betrokkenerelaties ON betrokkenerelaties.uuid = t.uuid 
            WHERE betrokkenerelaties.uuid IS NOT NULL)
        UPDATE betrokkenerelaties 
        SET rol = to_update.rol
        FROM to_update 
        WHERE to_update.uuid = betrokkenerelaties.uuid;"""

        with connection.cursor() as cursor:
            cursor.execute(update_query)

        return counter
