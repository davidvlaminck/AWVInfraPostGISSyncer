import json
import logging
import re
from typing import Iterator

import psycopg2

from Exceptions.AanleidingMissingError import AanleidingMissingError
from Exceptions.AssetMissingError import AssetMissingError
from Exceptions.RelatieTypeMissingError import RelatieTypeMissingError
from Helpers import peek_generator, turn_list_of_lists_into_string
from ResourceEnum import colorama_table, ResourceEnum


class AssetRelatiesUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, safe_insert: bool = False) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        objects_dict = {}

        values_array = []
        counter = 0
        for assetrelatie_dict in object_generator:
            counter += 1
            record_array = [
                f"'{assetrelatie_dict['@id'].replace('https://data.awvvlaanderen.be/id/assetrelatie/', '')[:36]}'",
                f"'{assetrelatie_dict['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[:36]}'",
                f"'{assetrelatie_dict['RelatieObject.doel']['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[:36]}'"]

            objects_dict[record_array[0][1:37]] = assetrelatie_dict['@type']
            objects_dict[record_array[1][1:37]] = assetrelatie_dict['RelatieObject.bron']['@type']
            objects_dict[record_array[2][1:37]] = assetrelatie_dict['RelatieObject.doel']['@type']

            if 'RelatieObject.typeURI' in assetrelatie_dict:
                record_array.append(f"'{assetrelatie_dict['RelatieObject.typeURI']}'")
            else:
                record_array.append(f"'{assetrelatie_dict['@type']}'")

            if 'AIMDBStatus.isActief' in assetrelatie_dict:
                record_array.append(f"{assetrelatie_dict['AIMDBStatus.isActief']}")
            else:
                record_array.append("TRUE")

            attributen_dict = assetrelatie_dict.copy()
            for key in ['@type', '@id', "RelatieObject.doel", "RelatieObject.assetId", "AIMDBStatus.isActief",
                        "RelatieObject.bronAssetId", "RelatieObject.doelAssetId", "RelatieObject.typeURI",
                        "RelatieObject.bron"]:
                attributen_dict.pop(key, None)
            attributen = json.dumps(attributen_dict).replace("'", "''")
            if attributen == '{}':
                attributen = ''

            if attributen == '':
                record_array.append("NULL")
            else:
                record_array.append(f"'{attributen}'")

            values_array.append(record_array)

        if len(values_array) == 0:
            return 0

        values_string = turn_list_of_lists_into_string(values_array)

        if safe_insert:
            insert_query = f"""
            WITH s (uuid, bronUuid, doelUuid, relatieTypeUri, actief, attributen) 
                AS (VALUES {values_string}),
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
            insert_query = f"""
            WITH s (uuid, bronUuid, doelUuid, relatieTypeUri, actief, attributen) 
                AS (VALUES {values_string}),
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
            with connection.cursor() as cursor:
                cursor.execute(insert_query)
        except psycopg2.errors.ForeignKeyViolation as exc:
            first_line = exc.args[0].split('\n')[0]
            if first_line == 'insert or update on table "assetrelaties" violates foreign key constraint "assetrelaties_bronuuid_fkey"':
                connection.rollback()
                if '\n' in str(exc):
                    error_detail = str(exc).split('\n')[1]
                    logging.error(error_detail)
                    problem_uuids = re.findall(r"bronuuid\)=\((.*?)\)", error_detail)
                    problem_uuid = problem_uuids[0]
                    problem_type = objects_dict[problem_uuid]
                    if problem_type.startswith('https://bz.data.wegenenverkeer.be/ns/aanleiding#'):
                        logging.error('raising AanleidingMissingError')
                        raise AanleidingMissingError()
                logging.error('raising AssetMissingError')
                raise AssetMissingError()
            elif first_line == 'insert or update on table "assetrelaties" violates foreign key constraint "assetrelaties_doeluuid_fkey"':
                connection.rollback()
                if '\n' in str(exc):
                    error_detail = str(exc).split('\n')[1]
                    logging.error(error_detail)
                    problem_uuids = re.findall(r"doeluuid\)=\((.*?)\)", error_detail)
                    problem_uuid = problem_uuids[0]
                    problem_type = objects_dict[problem_uuid]
                    if problem_type.startswith('https://bz.data.wegenenverkeer.be/ns/aanleiding#'):
                        logging.error('raising AanleidingMissingError')
                        raise AanleidingMissingError()
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
