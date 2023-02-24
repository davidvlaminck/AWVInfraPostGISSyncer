import json
import logging
from typing import Iterator

import psycopg2

from Exceptions.AgentMissingError import AgentMissingError
from Exceptions.AssetMissingError import AssetMissingError
from Helpers import peek_generator


class BetrokkeneRelatiesUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, safe_insert: bool = False) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values = ''
        counter = 0
        for betrokkenerelatie_dict in object_generator:
            counter += 1
            values += f"('{betrokkenerelatie_dict['@id'].replace('https://data.awvvlaanderen.be/id/assetrelatie/','')[0:36]}', " \
                      f"'{betrokkenerelatie_dict['RelatieObject.doel']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36]}', " \
                      f"'{betrokkenerelatie_dict['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36]}',"
            if 'HeeftBetrokkene.rol' in betrokkenerelatie_dict:
                values += f"'{betrokkenerelatie_dict['HeeftBetrokkene.rol'].replace('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlBetrokkenheidRol/','')}',"
            else:
                values += "NULL,"

            if betrokkenerelatie_dict['RelatieObject.bron']['@type'] == 'http://purl.org/dc/terms/Agent':
                values += f"'{betrokkenerelatie_dict['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36]}', NULL, "
            else:
                values += f"NULL, '{betrokkenerelatie_dict['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36]}', "

            if 'AIMDBStatus.isActief' in betrokkenerelatie_dict:
                values += f"{betrokkenerelatie_dict['AIMDBStatus.isActief']},"
            else:
                values += 'TRUE,'

            contact_info_value = 'NULL'
            if 'HeeftBetrokkene.specifiekeContactinfo' in betrokkenerelatie_dict:
                contact_info = betrokkenerelatie_dict['HeeftBetrokkene.specifiekeContactinfo']
                contact_info_value = "'" + json.dumps(contact_info).replace("'", "''") + "'"
            values += f"{contact_info_value},"
            
            start_datum = betrokkenerelatie_dict.get('HeeftBetrokkene.datumAanvang', None)
            eind_datum = betrokkenerelatie_dict.get('HeeftBetrokkene.datumEinde', None)

            if start_datum is None or start_datum == '':
                values += 'NULL,'
            else:
                values += f"'{start_datum}',"

            if eind_datum is None or eind_datum == '':
                values += 'NULL'
            else:
                values += f"'{eind_datum}'"

            values += "),"

        insert_query = f"""
        WITH s (uuid, doelUuid, bronUuid, rol, bronAgentUuid, bronAssetUuid, actief, contact_info, startDatum, 
            eindDatum) AS (VALUES {values[:-1]}),
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
                    logging.error(str(exc).split('\n')[1])
                connection.rollback()
                logging.error('raising AgentMissingError')
                raise AgentMissingError()

            elif first_line == 'insert or update on table "betrokkenerelaties" violates foreign key constraint "betrokkenerelaties_bron_assets_fkey"':
                if '\n' in str(exc):
                    logging.error(str(exc).split('\n')[1])
                connection.rollback()
                logging.error('raising AssetMissingError')
                raise AssetMissingError()
            else:
                connection.rollback()
                raise exc
        except Exception as exc:
            logging.error(f'raising unhandled error: {exc}')
            raise exc

        logging.info(f'done batch of {counter} betrokkenerelaties')
        return counter
