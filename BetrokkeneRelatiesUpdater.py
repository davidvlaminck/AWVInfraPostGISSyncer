import json
import logging
import re
from typing import Iterator

import psycopg2

from Exceptions.AgentMissingError import AgentMissingError
from Exceptions.AssetMissingError import AssetMissingError
from Helpers import peek_generator, turn_list_of_lists_into_string
from ResourceEnum import colorama_table, ResourceEnum


class BetrokkeneRelatiesUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, safe_insert: bool = False) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values_array = []
        counter = 0
        for betrokkenerelatie_dict in object_generator:
            counter += 1

            record_array = [
                f"'{betrokkenerelatie_dict['@id'].replace('https://data.awvvlaanderen.be/id/assetrelatie/', '')[0:36]}'",
                f"'{betrokkenerelatie_dict['RelatieObject.doel']['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]}'",
                f"'{betrokkenerelatie_dict['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]}'"]

            if 'HeeftBetrokkene.rol' in betrokkenerelatie_dict:
                record_array.append(f"'{betrokkenerelatie_dict['HeeftBetrokkene.rol'].replace('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlBetrokkenheidRol/','')}'")
            else:
                record_array.append("NULL")

            if betrokkenerelatie_dict['RelatieObject.bron']['@type'] == 'http://purl.org/dc/terms/Agent':
                record_array.append(f"'{betrokkenerelatie_dict['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36]}'")
                record_array.append("NULL")
            else:
                record_array.append("NULL")
                record_array.append(f"'{betrokkenerelatie_dict['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36]}'")

            if 'AIMDBStatus.isActief' in betrokkenerelatie_dict:
                record_array.append(f"{betrokkenerelatie_dict['AIMDBStatus.isActief']}")
            else:
                record_array.append("TRUE")

            contact_info_value = 'NULL'
            if 'HeeftBetrokkene.specifiekeContactinfo' in betrokkenerelatie_dict:
                contact_info = betrokkenerelatie_dict['HeeftBetrokkene.specifiekeContactinfo']
                contact_info_value = "'" + json.dumps(contact_info).replace("'", "''") + "'"
            record_array.append(contact_info_value)

            start_datum = betrokkenerelatie_dict.get('HeeftBetrokkene.datumAanvang', None)
            eind_datum = betrokkenerelatie_dict.get('HeeftBetrokkene.datumEinde', None)

            if start_datum is None or start_datum == '':
                record_array.append("NULL")
            else:
                record_array.append(f"'{start_datum}'")

            if eind_datum is None or eind_datum == '':
                record_array.append("NULL")
            else:
                record_array.append(f"'{eind_datum}'")

            values_array.append(record_array)

        if len(values_array) == 0:
            return 0

        values_string = turn_list_of_lists_into_string(values_array)

        insert_query = f"""
        WITH s (uuid, doelUuid, bronUuid, rol, bronAgentUuid, bronAssetUuid, actief, contact_info, startDatum, 
            eindDatum) AS (VALUES {values_string}),
        to_insert AS (
            SELECT uuid::uuid AS uuid, doelUuid::uuid AS doelUuid, bronUuid::uuid AS bronUuid, rol, 
                bronAgentUuid::uuid AS bronAgentUuid, bronAssetUuid::uuid AS bronAssetUuid, actief, 
                contact_info::json as contact_info, startDatum::TIMESTAMP as startDatum, 
                eindDatum::TIMESTAMP as eindDatum
            FROM s)        
        INSERT INTO public.betrokkeneRelaties (uuid, doelUuid, bronUuid, rol, bronAgentUuid, bronAssetUuid, actief, 
            contact_info, startDatum, eindDatum) 
        SELECT to_insert.uuid, to_insert.doelUuid, to_insert.bronUuid, to_insert.rol, to_insert.bronAgentUuid, 
            to_insert.bronAssetUuid, to_insert.actief, to_insert.contact_info, to_insert.startDatum, to_insert.eindDatum
        FROM to_insert;"""

        try:
            with connection.cursor() as cursor:
                cursor.execute(insert_query)
        except psycopg2.Error as exc:
            first_line = exc.args[0].split('\n')[0]
            if first_line in ['insert or update on table "betrokkenerelaties" violates foreign key constraint "betrokkenerelaties_agents_fkey"',
                              'insert or update on table "betrokkenerelaties" violates foreign key constraint "betrokkenerelaties_bron_agents_fkey"']:
                if '\n' in str(exc):
                    logging.error(colorama_table[ResourceEnum.betrokkenerelaties] + str(exc).split('\n')[1])
                connection.rollback()
                logging.error(colorama_table[ResourceEnum.betrokkenerelaties] + 'raising AgentMissingError')
                raise AgentMissingError()

            elif first_line == 'insert or update on table "betrokkenerelaties" violates foreign key constraint "betrokkenerelaties_bron_assets_fkey"':
                exception_to_raise = AssetMissingError()
                if '\n' in str(exc):
                    detail_line = str(exc).split('\n')[1]
                    exception_to_raise.asset_uuids = re.findall(r"bronassetuuid\)=\((.*?)\)", detail_line)
                    logging.error(f'{colorama_table[ResourceEnum.betrokkenerelaties]}{detail_line}')
                connection.rollback()
                logging.error(colorama_table[ResourceEnum.betrokkenerelaties] + 'raising AssetMissingError')
                raise exception_to_raise
            else:
                connection.rollback()
                raise exc
        except Exception as exc:
            logging.error(colorama_table[ResourceEnum.betrokkenerelaties] + f'raising unhandled error: {exc}')
            raise exc

        logging.info(colorama_table[ResourceEnum.betrokkenerelaties] + f'done batch of {counter} betrokkenerelaties')
        return counter
