import json
import logging
import time
from typing import Iterator

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Helpers import chunked, peek_generator


class AssetrelatieEigenschappenGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info('started changing eigenschappen of assetrelaties')
        start = time.time()

        assetrelatie_count = 0
        for uuids_chunk in chunked(uuids, 100):
            generator = self.eminfra_importer.import_resource_from_webservice_by_uuids(uuids=uuids_chunk,
                                                                                       resource='assetrelaties')

            assetrelatie_count += self.update_eigenschappen(object_generator=generator, connection=connection)

        end = time.time()
        logging.info(f'changed eigenschappen of {assetrelatie_count} assetrelaties in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def update_eigenschappen(object_generator: Iterator[dict], connection) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values = ''
        counter = 0
        for assetrelatie_dict in object_generator:
            if assetrelatie_dict is None:
                continue
            counter += 1
            assetrelatie_uuid = assetrelatie_dict['@id'].split('/')[-1][0:36]

            attributen_dict = assetrelatie_dict.copy()
            for key in ['@type', '@id', "RelatieObject.doel", "RelatieObject.assetId", "AIMDBStatus.isActief",
                        "RelatieObject.bronAssetId", "RelatieObject.doelAssetId", "RelatieObject.typeURI",
                        "RelatieObject.bron"]:
                attributen_dict.pop(key, None)
            attributen = json.dumps(attributen_dict).replace("'", "''")
            if attributen == '{}':
                attributen = ''

            if attributen == '':
                values += f"('{assetrelatie_uuid}',NULL),"
            else:
                values += f"('{assetrelatie_uuid}','{attributen}'),"

        if values == '':
            return counter

        update_query = f"""
        WITH s (uuid, attributen) 
            AS (VALUES {values[:-1]}),
        t AS (
            SELECT uuid::uuid AS uuid, attributen::json AS attributen
            FROM s),
        to_update AS (
            SELECT t.* 
            FROM t
                LEFT JOIN public.assetrelaties AS assetrelaties ON assetrelaties.uuid = t.uuid 
            WHERE assetrelaties.uuid IS NOT NULL)
        UPDATE assetrelaties 
        SET attributen = to_update.attributen
        FROM to_update 
        WHERE to_update.uuid = assetrelaties.uuid;"""

        with connection.cursor() as cursor:
            cursor.execute(update_query)

        return counter
