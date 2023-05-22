import concurrent.futures
import logging
import time
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor

import psycopg2

from EMInfraImporter import EMInfraImporter
from Exceptions.BestekMissingError import BestekMissingError
from PostGISConnector import PostGISConnector
from ResourceEnum import ResourceEnum, colorama_table


# maak gebruik van legacy installaties endpoint:
# {"size":10,"from":0,"selection":{"expressions":[{"terms":[{"property":"actief","value":true,"operator":0,"logicalOp":null,"negate":false}],"logicalOp":null}],"settings":{}},"expansions":{"fields":["parent","kenmerk:ee2e627e-bb79-47aa-956a-ea167d20acbd"]},"pagingMode":"CURSOR","fromCursor":null}

class BestekKoppelingSyncer:
    def __init__(self, postGIS_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postGIS_connector = postGIS_connector
        self.eminfra_importer = em_infra_importer
        self.color = colorama_table[ResourceEnum.bestekkoppelingen]

    def sync_bestekkoppelingen(self, batch_size: int = 100):
        self.update_all_bestekkoppelingen(batch_size=batch_size)

    def update_all_bestekkoppelingen(self, batch_size: int):
        connection = self.postGIS_connector.get_connection()
        params = self.postGIS_connector.get_params(connection)

        if params['bestekkoppelingen_cursor'] == '':
            # create a temp table that holds all asset_uuid
            self.create_temp_table_for_sync_bestekkoppelingen(connection=connection)
            self.postGIS_connector.update_params(params={'bestekkoppelingen_cursor': 'setup done'},
                                                 connection=connection)
            params = self.postGIS_connector.get_params(connection)

        if params['bestekkoppelingen_cursor'] == 'setup done':
            # go through all of the table and flag as sync'd when done, allow for parameter batch size
            self.loop_using_temp_table_and_sync_koppelingen(batch_size=batch_size, connection=connection)
            self.postGIS_connector.update_params(params={'bestekkoppelingen_cursor': 'syncing done'},
                                                 connection=connection)
            params = self.postGIS_connector.get_params(connection)

        if params['bestekkoppelingen_cursor'] == 'syncing done':
            # delete the temp table
            self.delete_temp_table_for_sync_bestekkoppelingen(connection=connection)
            self.postGIS_connector.update_params(params={'bestekkoppelingen_fill': False}, connection=connection)

        connection.close()

    def get_all_bestekkoppelingen_by_asset_uuids_onderdelen(self, asset_uuids: [str]) -> Generator[tuple]:
        yield from self.eminfra_importer.get_all_bestekkoppelingen_from_webservice_by_asset_uuids_onderdelen(
            asset_uuids=asset_uuids)

    def get_all_bestekkoppelingen_by_asset_uuids_installaties(self, asset_uuids: [str]) -> Generator[tuple]:
        yield from self.eminfra_importer.get_all_bestekkoppelingen_from_webservice_by_asset_uuids_installaties(
            asset_uuids=asset_uuids)

    @staticmethod
    def update_bestekkoppelingen_by_asset_uuids(connection, asset_uuids: [str],
                                                bestek_koppelingen_dicts_list: [[dict]]) -> None:
        if len(asset_uuids) == 0:
            return

        delete_query = "DELETE FROM public.bestekkoppelingen WHERE assetUuid IN (VALUES ('" + "'::uuid),('".join(
            asset_uuids) + "'::uuid));"
        with connection.cursor() as cursor:
            cursor.execute(delete_query)

            for index, asset_uuid in enumerate(asset_uuids):
                values = ''
                bestek_koppelingen_dicts = bestek_koppelingen_dicts_list[index]

                if len(bestek_koppelingen_dicts) == 0:
                    continue

                for bestek_koppeling_dict in bestek_koppelingen_dicts:
                    bestek_uuid = bestek_koppeling_dict['bestekRef']['uuid']
                    start_datum = bestek_koppeling_dict['startDatum']
                    eind_datum = bestek_koppeling_dict.get('eindDatum', None)
                    koppeling_status = bestek_koppeling_dict['status']

                    values += f"('{asset_uuid}','{bestek_uuid}','{start_datum}',"
                    if eind_datum is None:
                        values += 'NULL'
                    else:
                        values += f"'{eind_datum}'"
                    values += f", '{koppeling_status}'),"

                if values == '':
                    continue

                insert_query = f"""
    WITH s (assetUuid, bestekUuid, startDatum, eindDatum, koppelingStatus) 
        AS (VALUES {values[:-1]}),
    to_insert AS (
        SELECT assetUuid::uuid AS assetUuid, bestekUuid::uuid AS bestekUuid, startDatum::TIMESTAMP as startDatum, eindDatum::TIMESTAMP as eindDatum, koppelingStatus
        FROM s)
    INSERT INTO public.bestekkoppelingen (assetUuid, bestekUuid, startDatum, eindDatum, koppelingStatus) 
    SELECT to_insert.assetUuid, to_insert.bestekUuid, to_insert.startDatum, to_insert.eindDatum, to_insert.koppelingStatus
    FROM to_insert;"""

                try:
                    cursor.execute(insert_query)
                except psycopg2.Error as exc:
                    if str(exc).split('\n')[0] == 'insert or update on table "bestekkoppelingen" violates foreign key ' \
                                                  'constraint "bestekkoppelingen_bestekken_fkey"':
                        raise BestekMissingError()
                    else:
                        raise exc

    @staticmethod
    def create_temp_table_for_sync_bestekkoppelingen(connection):
        create_table_query = """
CREATE TABLE IF NOT EXISTS public.temp_sync_bestekkoppelingen
(
    assetUuid uuid NOT NULL,
    done boolean
);"""
        check_table_query = "SELECT count(*) FROM public.temp_sync_bestekkoppelingen WHERE done IS NULL;"

        fill_table_query = """
WITH bestek_assets AS (
    SELECT assets.uuid FROM public.assettypes 
        INNER JOIN public.assets on assets.assettype = assettypes.uuid
    WHERE bestek = TRUE)
INSERT INTO public.temp_sync_bestekkoppelingen (assetUuid) 
SELECT uuid FROM bestek_assets;"""

        with connection.cursor() as cursor:
            cursor.execute(create_table_query)

            # only fill_table if the number of records in temp table with NULL for done == 0 (empty or done)
            cursor.execute(check_table_query)
            count_not_done = cursor.fetchone()[0]
            if count_not_done > 0:
                return

            cursor.execute(fill_table_query)
        connection.commit()

    @staticmethod
    def delete_temp_table_for_sync_bestekkoppelingen(connection):
        delete_table_query = "DROP TABLE IF EXISTS public.temp_sync_bestekkoppelingen;"

        with connection.cursor() as cursor:
            cursor.execute(delete_table_query)
        connection.commit()

    def update_bestekkoppelingen_by_asset_uuid(self, asset_uuid: str,
                                               koppelingen: [dict]) -> None:
        start = time.time()
        logging.info(f'starting proces for {asset_uuid}')
        connection = None
        try:
            with self.postGIS_connector.get_connection() as connection:
                self.update_bestekkoppelingen_by_asset_uuids(connection=connection, asset_uuids=[asset_uuid],
                                                             bestek_koppelingen_dicts_list=[list(koppelingen)])
                with connection.cursor() as cursor:
                    update_temp_table_query = "UPDATE public.temp_sync_bestekkoppelingen SET done = TRUE WHERE " \
                                              "assetUuid = '" + asset_uuid + "'::uuid;"
                    cursor.execute(update_temp_table_query)
            end = time.time()
            logging.info(self.color + f'updated bestekkoppelingen for 1 asset in {str(round(end - start, 2))} seconds.')
        except Exception as exc:
            print(exc.args[0])
            pass
        finally:
            if connection is not None:
                self.postGIS_connector.kill_connection(connection)

    def loop_using_temp_table_and_sync_koppelingen(self, batch_size: int, connection):
        select_from_temp_table_query = "SELECT assetuuid, CASE WHEN t.uri LIKE '%/ns/onderdeel#%' THEN 'onderdelen' ELSE 'installaties' END AS asset_cat " \
                                       "FROM public.temp_sync_bestekkoppelingen " \
                                       "LEFT JOIN assets a ON a.uuid = assetUuid " \
                                       "LEFT JOIN assettypes t ON a.assettype = t.uuid " \
                                       "WHERE done IS NULL " \
                                       f"LIMIT {batch_size}; "
        with connection.cursor() as cursor:
            start = time.time()
            cursor.execute(select_from_temp_table_query)
            all_rows = cursor.fetchall()
            onderdelen_to_update = list(map(lambda x: x[0], filter(lambda x: x[1] == 'onderdelen', all_rows)))
            installaties_to_update = list(map(lambda x: x[0], filter(lambda x: x[1] == 'installaties', all_rows)))
            while len(onderdelen_to_update) > 0 or len(installaties_to_update) > 0:
                koppelingen_generator = self.get_all_bestekkoppelingen_by_asset_uuids_onderdelen(
                    asset_uuids=onderdelen_to_update)
                # use multithreading
                executor = ThreadPoolExecutor(8)
                futures = [executor.submit(self.update_bestekkoppelingen_by_asset_uuid, asset_uuid=asset_uuid,
                                           koppelingen=koppelingen)
                           for asset_uuid, koppelingen in koppelingen_generator]
                concurrent.futures.wait(futures)

                koppelingen_generator = self.get_all_bestekkoppelingen_by_asset_uuids_installaties(
                    asset_uuids=installaties_to_update)
                executor = ThreadPoolExecutor(8)
                futures = [executor.submit(self.update_bestekkoppelingen_by_asset_uuid, asset_uuid=asset_uuid,
                                           koppelingen=koppelingen)
                           for asset_uuid, koppelingen in koppelingen_generator]
                concurrent.futures.wait(futures)
                connection.commit()

                cursor.execute(select_from_temp_table_query)
                onderdelen_to_update = list(map(lambda x: x[0], filter(lambda x: x[1] == 'onderdelen', all_rows)))
                installaties_to_update = list(map(lambda x: x[0], filter(lambda x: x[1] == 'installaties', all_rows)))

            end = time.time()
            logging.info(
                self.color + f'updated {batch_size} bestekkoppelingen in {str(round(end - start, 2))} seconds.')
