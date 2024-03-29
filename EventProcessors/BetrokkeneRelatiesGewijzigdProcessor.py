import logging
import time

import psycopg2

from EMInfraImporter import EMInfraImporter
from EventProcessors.SpecificEventProcessor import SpecificEventProcessor
from Exceptions.AgentMissingError import AgentMissingError
from PostGISConnector import PostGISConnector


class BetrokkeneRelatiesGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, em_infra_importer: EMInfraImporter, connector: PostGISConnector):
        super().__init__(cursor, em_infra_importer)
        self.connector = connector

    def process(self, uuids: [str]):
        logging.info(f'started updating betrokkenerelaties')
        start = time.time()

        betrokkenerelatie_dicts = self.em_infra_importer.import_betrokkenerelaties_from_webservice_by_assetuuids(asset_uuids=uuids)
        self.remove_all_betrokkene_relaties(cursor=self.cursor, bron_uuids=list(set(uuids)))
        self.process_dicts(cursor=self.cursor, betrokkenerelatie_dicts=betrokkenerelatie_dicts)

        end = time.time()
        logging.info(f'updated {len(betrokkenerelatie_dicts)} betrokkenerelaties in {str(round(end - start, 2))} seconds.')

    def process_dicts(self, cursor, betrokkenerelatie_dicts: dict):
        if len(betrokkenerelatie_dicts) == 0:
            return

        logging.info(f'started creating {len(betrokkenerelatie_dicts)} betrokkenerelaties')
        values = ''

        for betrokkenerelatie_dict in betrokkenerelatie_dicts:
            values += f"('{betrokkenerelatie_dict['@id'].replace('https://data.awvvlaanderen.be/id/assetrelatie/','')[0:36]}', '{betrokkenerelatie_dict['RelatieObject.doel']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36]}', " \
                      f"'{betrokkenerelatie_dict['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36]}',"
            if 'HeeftBetrokkene.rol' in betrokkenerelatie_dict:
                values += f"'{betrokkenerelatie_dict['HeeftBetrokkene.rol'].replace('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlBetrokkenheidRol/','')}',"
            else:
                values += "NULL,"
            values += "TRUE),"

        insert_query = f"""
        WITH s (uuid, doelUuid, bronUuid, rol, actief) 
           AS (VALUES {values[:-1]}),
        to_insert AS (
           SELECT uuid::uuid AS uuid, doelUuid::uuid AS doelUuid, bronUuid::uuid AS bronUuid, rol, actief
           FROM s)        
        INSERT INTO public.betrokkeneRelaties (uuid, doelUuid, bronUuid, rol, actief) 
        SELECT to_insert.uuid, to_insert.doelUuid, to_insert.bronUuid, to_insert.rol, to_insert.actief
        FROM to_insert;"""

        try:
            cursor.execute(insert_query)
        except psycopg2.Error as exc:
            if str(exc).split('\n')[0] == 'insert or update on table "betrokkenerelaties" violates foreign key constraint "betrokkenerelaties_agents_fkey"':
                if '\n' in str(exc):
                    print(str(exc).split('\n')[1])
                self.connector.connection.rollback()
                cursor = self.connector.connection.cursor()
                agent_uuids = set(map(lambda x: x['RelatieObject.doel']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36], betrokkenerelatie_dicts))
                select_agents_query = f"""SELECT uuid FROM public.agents WHERE uuid IN ('{"'::uuid,'".join(agent_uuids)}'::uuid)"""
                cursor.execute(select_agents_query)
                existing_agent_uuids = set(map(lambda x: x[0], cursor.fetchall()))
                nonexisting_agents = list(agent_uuids - existing_agent_uuids)
                raise AgentMissingError(nonexisting_agents)
            else:
                raise exc
        logging.info('done batch of betrokkenerelaties')

    def remove_all_betrokkene_relaties(self, cursor, bron_uuids):
        if len(bron_uuids) == 0:
            return
        cursor.execute(f"""
        DELETE FROM public.betrokkenerelaties 
        WHERE bronUuid in ('{"','".join(bron_uuids)}')""")
