import json
import logging
from typing import Iterator

import psycopg2

from Exceptions.AssetMissingError import AssetMissingError
from Exceptions.RelatieTypeMissingError import RelatieTypeMissingError
from Helpers import peek_generator
from ResourceEnum import colorama_table, ResourceEnum
from constants import ASSETRELATIE_URL_PREFIX, ASSET_URL_PREFIX


class AssetRelatiesUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, safe_insert: bool = False) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values_array = []
        counter = 0
        for assetrelatie_dict in object_generator:
            counter += 1
            record_array = [
                assetrelatie_dict['@id'].replace(ASSETRELATIE_URL_PREFIX, '')[:36],
                assetrelatie_dict['RelatieObject.bron']['@id'].replace(ASSET_URL_PREFIX, '')[:36],
                assetrelatie_dict['RelatieObject.doel']['@id'].replace(ASSET_URL_PREFIX, '')[:36]]

            if 'RelatieObject.typeURI' in assetrelatie_dict:
                record_array.append(assetrelatie_dict['RelatieObject.typeURI'])
            else:
                record_array.append(assetrelatie_dict['@type'])

            if 'AIMDBStatus.isActief' in assetrelatie_dict:
                record_array.append(assetrelatie_dict['AIMDBStatus.isActief'])
            else:
                record_array.append(True)

            attributen_dict = assetrelatie_dict.copy()
            for key in ['@type', '@id', "RelatieObject.doel", "RelatieObject.assetId", "AIMDBStatus.isActief",
                        "RelatieObject.bronAssetId", "RelatieObject.doelAssetId", "RelatieObject.typeURI",
                        "RelatieObject.bron"]:
                attributen_dict.pop(key, None)
            attributen = json.dumps(attributen_dict)
            if attributen == '{}':
                attributen = None

            record_array.append(attributen)

            values_array.append(record_array)

        if len(values_array) == 0:
            return 0

        if safe_insert:
            insert_query = """
            WITH s (uuid, bronUuid, doelUuid, relatieTypeUri, actief, attributen) 
                AS (VALUES %s),
            to_insert AS (
                SELECT s.uuid::uuid AS uuid, bronUuid::uuid AS bronUuid, doelUuid::uuid AS doelUuid, 
                    relatietypes.uuid as relatietype, s.actief, attributen::json as attributen
                FROM s
                    LEFT JOIN relatietypes ON relatietypes.uri = s.relatieTypeUri
                    INNER JOIN assets a1 on bronUuid::uuid = a1.uuid
                    INNER JOIN assets a2 on doelUuid::uuid = a2.uuid)        
            INSERT INTO public.assetrelaties (uuid, bronUuid, doelUuid, relatietype, actief, attributen) 
            SELECT to_insert.uuid, to_insert.bronUuid, to_insert.doelUuid, to_insert.relatietype, to_insert.actief, to_insert.attributen
            FROM to_insert;"""
        else:
            insert_query = """
            WITH s (uuid, bronUuid, doelUuid, relatieTypeUri, actief, attributen) 
                AS (VALUES %s),
            to_insert AS (
                SELECT s.uuid::uuid AS uuid, s.bronUuid::uuid AS bronUuid, s.doelUuid::uuid AS doelUuid, 
                    relatietypes.uuid as relatietype, s.actief, s.attributen::json as attributen
                FROM s
                    LEFT JOIN relatietypes ON relatietypes.uri = s.relatieTypeUri
                    LEFT JOIN assetrelaties ar ON ar.uuid = s.uuid::uuid
                WHERE ar.uuid IS NULL)        
            INSERT INTO public.assetrelaties (uuid, bronUuid, doelUuid, relatietype, actief, attributen) 
            SELECT to_insert.uuid, to_insert.bronUuid, to_insert.doelUuid, to_insert.relatietype, to_insert.actief, to_insert.attributen
            FROM to_insert;"""

        try:
            from psycopg2.extras import execute_values
            with connection.cursor() as cursor:
                execute_values(cursor, insert_query, values_array)
        except psycopg2.errors.ForeignKeyViolation as exc:
            first_line = exc.args[0].split('\n')[0]
            if first_line == 'insert or update on table "assetrelaties" violates foreign key constraint "assetrelaties_bronuuid_fkey"':
                if '\n' in str(exc):
                    logging.error(str(exc).split('\n')[1])
                connection.rollback()
                logging.error('raising AssetMissingError')
                raise AssetMissingError()
            elif first_line == 'insert or update on table "assetrelaties" violates foreign key constraint "assetrelaties_doeluuid_fkey"':
                if '\n' in str(exc):
                    logging.error(str(exc).split('\n')[1])
                connection.rollback()
                logging.error('raising AssetMissingError')
                raise AssetMissingError()
            elif first_line == 'insert or update on table "assetrelaties" violates foreign key constraint "assetrelaties_relatietype_fkey"':
                if '\n' in str(exc):
                    logging.error(str(exc).split('\n')[1])
                connection.rollback()
                logging.error('raising RelatieTypeMissingError')
                raise RelatieTypeMissingError()
            else:
                connection.rollback()
                raise exc
        except psycopg2.errors.NotNullViolation as exc:
            first_line = exc.args[0].split('\n')[0]
            if 'null value in column "relatietype"' in first_line and 'violates not-null constraint' in first_line:
                if '\n' in str(exc):
                    logging.error(str(exc).split('\n')[1])
                connection.rollback()
                logging.error('raising RelatieTypeMissingError')
                raise RelatieTypeMissingError()
            else:
                connection.rollback()
                raise exc
        except Exception as exc:
            logging.error(f'{colorama_table[ResourceEnum.assetrelaties]}raising unhandled error: {exc}')
            raise exc

        logging.info(f'{colorama_table[ResourceEnum.assetrelaties]}done batch of {counter} assetrelaties')
        return counter
