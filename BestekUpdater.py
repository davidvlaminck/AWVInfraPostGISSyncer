import logging
from typing import Iterator

from Helpers import peek_generator


class BestekUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, safe_insert: bool = False):
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return

        values = ''
        for bestek_dict in object_generator:
            try:
                uuid = bestek_dict['uuid']
                eDeltaDossiernummer = bestek_dict.get('eDeltaDossiernummer', None)
                if eDeltaDossiernummer is None and 'nummer' in bestek_dict:
                    eDeltaDossiernummer = bestek_dict['nummer']
                eDeltaBesteknummer = bestek_dict.get('eDeltaBesteknummer', None)
                if eDeltaBesteknummer is None and 'nummer' in bestek_dict:
                    eDeltaBesteknummer = bestek_dict['nummer']
                aannemerNaam = None
                if 'aannemerNaam' in bestek_dict:
                    aannemerNaam = bestek_dict['aannemerNaam'].replace("'", "''")
                values += f"('{uuid}','{eDeltaDossiernummer}','{eDeltaBesteknummer}',"
                if aannemerNaam is not None:
                    values += f"'{aannemerNaam}'"
                else:
                    values += "NULL"
                values += "),"
            except KeyError as exc:
                logging.error(f'Could not create a bestek from the following respoonse:\n{bestek_dict}\nError:{exc}')
                continue

        insert_query = f"""
        WITH s (uuid, eDeltaDossiernummer, eDeltaBesteknummer, aannemerNaam) 
            AS (VALUES {values[:-1]}),
        t AS (
            SELECT uuid::uuid AS uuid, eDeltaDossiernummer, eDeltaBesteknummer, aannemerNaam
            FROM s),
        to_insert AS (
            SELECT t.* 
            FROM t
                LEFT JOIN public.bestekken AS bestekken ON bestekken.uuid = t.uuid 
            WHERE bestekken.uuid IS NULL)
        INSERT INTO public.bestekken (uuid, eDeltaDossiernummer, eDeltaBesteknummer, aannemerNaam)
        SELECT to_insert.uuid, to_insert.eDeltaDossiernummer, to_insert.eDeltaBesteknummer, to_insert.aannemerNaam
        FROM to_insert;"""

        update_query = f"""
        WITH s (uuid, eDeltaDossiernummer, eDeltaBesteknummer, aannemerNaam) 
            AS (VALUES {values[:-1]}),
        t AS (
            SELECT uuid::uuid AS uuid, eDeltaDossiernummer, eDeltaBesteknummer, aannemerNaam
            FROM s),
        to_update AS (
            SELECT t.* 
            FROM t
                LEFT JOIN public.bestekken AS bestekken ON bestekken.uuid = t.uuid 
            WHERE bestekken.uuid IS NOT NULL)
        UPDATE bestekken 
        SET eDeltaDossiernummer = to_update.eDeltaDossiernummer, eDeltaBesteknummer = to_update.eDeltaBesteknummer, 
            aannemerNaam = to_update.aannemerNaam
        FROM to_update 
        WHERE to_update.uuid = bestekken.uuid;"""

        cursor = connection.cursor()
        cursor.execute(insert_query)

        cursor = connection.cursor()
        cursor.execute(update_query)
