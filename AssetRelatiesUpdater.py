import json
import logging
from typing import Iterator

import psycopg2

from Exceptions.AgentMissingError import AgentMissingError
from Exceptions.AssetMissingError import AssetMissingError
from Helpers import peek_generator


class AssetRelatiesUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values = ''
        counter = 0
        for assetrelatie_dict in object_generator:
            counter += 1
            values += f"('{assetrelatie_dict['@id'].replace('https://data.awvvlaanderen.be/id/assetrelatie/','')[0:36]}', '{assetrelatie_dict['RelatieObject.doel']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36]}', " \
                      f"'{assetrelatie_dict['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36]}',"

            if 'RelatieObject.typeURI' in assetrelatie_dict:
                relatie_type_uri = assetrelatie_dict['RelatieObject.typeURI']
            else:
                relatie_type_uri = assetrelatie_dict['@type']

            values += f"'{relatie_type_uri}',{assetrelatie_dict['AIMDBStatus.isActief']},"

            attributen_dict = assetrelatie_dict.copy()
            for key in ['@type', '@id', "RelatieObject.doel", "RelatieObject.assetId", "AIMDBStatus.isActief",
                        "RelatieObject.bronAssetId", "RelatieObject.doelAssetId", "RelatieObject.typeURI",
                        "RelatieObject.bron"]:
                attributen_dict.pop(key, None)
            attributen = json.dumps(attributen_dict).replace("'", "''")
            if attributen == '{}':
                attributen = ''

            if attributen == '':
                values += "NULL),"
            else:
                values += f"'{attributen}'),"

        insert_query = f"""
        WITH s (uuid, bronUuid, doelUuid, relatieTypeUri, actief, attributen) 
            AS (VALUES {values[:-1]}),
        to_insert AS (
            SELECT s.uuid::uuid AS uuid, bronUuid::uuid AS bronUuid, doelUuid::uuid AS doelUuid, 
                relatietypes.uuid as relatietype, s.actief, attributen::json as attributen
            FROM s
                LEFT JOIN relatietypes ON relatietypes.uri = s.relatieTypeUri)        
        INSERT INTO public.assetrelaties (uuid, bronUuid, doelUuid, relatietype, actief, attributen) 
        SELECT to_insert.uuid, to_insert.bronUuid, to_insert.doelUuid, to_insert.relatietype, to_insert.actief, to_insert.attributen
        FROM to_insert;"""

        try:
            with connection.cursor() as cursor:
                cursor.execute(insert_query)
        except psycopg2.Error as exc:
            if str(exc).split('\n')[0] in ['insert or update on table "betrokkenerelaties" violates foreign key constraint "betrokkenerelaties_agents_fkey"',
                                           'insert or update on table "betrokkenerelaties" violates foreign key constraint "betrokkenerelaties_bron_agents_fkey"']:
                if '\n' in str(exc):
                    logging.error(str(exc).split('\n')[1])
                connection.rollback()
                raise AgentMissingError()
                cursor = connection.cursor()
                agent_uuids = set(map(lambda x: x['RelatieObject.doel']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36], betrokkenerelatie_dicts_list))
                more_agent_uuids = set(map(lambda x: x['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36],
                    filter(lambda x: x['RelatieObject.bron']['@type'] == 'http://purl.org/dc/terms/Agent', betrokkenerelatie_dicts_list)))
                agent_uuids.union(more_agent_uuids)
                select_agents_query = f"""SELECT uuid FROM public.agents WHERE uuid IN ('{"'::uuid,'".join(agent_uuids)}'::uuid);"""
                cursor.execute(select_agents_query)
                existing_agent_uuids = set(map(lambda x: x[0], cursor.fetchall()))
                nonexisting_agents = list(agent_uuids - existing_agent_uuids)
                raise AgentMissingError(nonexisting_agents)
            elif str(exc).split('\n')[0] == 'insert or update on table "betrokkenerelaties" violates foreign key constraint "betrokkenerelaties_bron_assets_fkey"':
                if '\n' in str(exc):
                    logging.error(str(exc).split('\n')[1])
                connection.rollback()
                raise AssetMissingError()
                cursor = connection.cursor()
                bron_uuids = set(map(lambda x: x['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36], betrokkenerelatie_dicts_list))
                asset_uuids = set(
                    filter(lambda x: x['RelatieObject.bron']['@type'] != 'http://purl.org/dc/terms/Agent', bron_uuids))

                select_assets_query = f"""SELECT uuid FROM public.assets WHERE uuid IN ('{"'::uuid,'".join(asset_uuids)}'::uuid);"""
                cursor.execute(select_assets_query)
                existing_asset_uuids = set(map(lambda x: x[0], cursor.fetchall()))
                nonexisting_assets = list(asset_uuids - existing_asset_uuids)
                raise AssetMissingError(nonexisting_assets)
            else:
                raise exc

        logging.info(f'done batch of {counter} betrokkenerelaties')
        return counter
