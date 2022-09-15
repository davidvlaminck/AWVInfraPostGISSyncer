import logging
import time

from EMInfraImporter import EMInfraImporter
from EventProcessors.RelatieProcessor import RelatieProcessor
from EventProcessors.RelationNotCreatedError import RelationNotCreatedError, BetrokkeneRelationNotCreatedError
from EventProcessors.SpecificEventProcessor import SpecificEventProcessor


class BetrokkeneRelatiesGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, em_infra_importer: EMInfraImporter):
        super().__init__(cursor, em_infra_importer)

    def process(self, uuids: [str]):
        logging.info(f'started updating betrokkenerelaties')
        start = time.time()

        betrokkenerelatie_dicts = self.em_infra_importer.import_betrokkenerelaties_from_webservice_by_assetuuids(asset_uuids=uuids)
        self.process_dicts(cursor=self.cursor, asset_uuids=uuids, betrokkenerelatie_dicts=betrokkenerelatie_dicts)

        end = time.time()
        logging.info(f'updated {len(betrokkenerelatie_dicts)} assets in {str(round(end - start, 2))} seconds.')

    def process_dicts(self, cursor, asset_uuids: [str], betrokkenerelatie_dicts: dict):
        logging.info(f'started creating {len(betrokkenerelatie_dicts)} betrokkenerelaties')
        self.remove_all_betrokkene_relaties(cursor=cursor, asset_uuids=list(set(asset_uuids)))

        values = ''
        for betrokkenerelatie_dict in betrokkenerelatie_dicts:
            values += f"('{betrokkenerelatie_dict['uuid']}', '{betrokkenerelatie_dict['doel']['uuid']}', " \
                      f"'{betrokkenerelatie_dict['bron']['uuid']}',"
            if 'rol' in betrokkenerelatie_dict:
                values += f"'{betrokkenerelatie_dict['rol']}',"
            else:
                values += "NULL,"
            values += "TRUE),"

        insert_query = f"""
        WITH s (uuid, agentUuid, assetUuid, rol, actief) 
           AS (VALUES {values[:-1]}),
        to_insert AS (
           SELECT uuid::uuid AS uuid, agentUuid::uuid AS agentUuid, assetUuid::uuid AS assetUuid, rol, actief
           FROM s)        
        INSERT INTO public.betrokkeneRelaties (uuid, agentUuid, assetUuid, rol, actief) 
        SELECT to_insert.uuid, to_insert.agentUuid, to_insert.assetUuid, to_insert.rol, to_insert.actief
        FROM to_insert;"""

        cursor.execute(insert_query)

        logging.info('done')

    def remove_all_betrokkene_relaties(self, cursor, asset_uuids):
        if len(asset_uuids) == 0:
            return
        cursor.execute(f"""
        DELETE FROM betrokkenerelaties 
        WHERE assetUuid in ('{"','".join(asset_uuids)}')""")
