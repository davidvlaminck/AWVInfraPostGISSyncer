import json
from collections.abc import Generator

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class BestekKoppelingSyncer:
    def __init__(self, postGIS_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postGIS_connector = postGIS_connector
        self.eminfra_importer = em_infra_importer

    def sync_bestekkoppelingen(self, batch_size: int = 20):
        self.update_all_bestekkoppelingen(batch_size=batch_size)

    def update_all_bestekkoppelingen(self, batch_size: int):
        # create a temp table that holds all asset_uuid
        self.create_temp_table_for_sync_bestekkoppelingen()

        # go through all of the table and flag as sync'd when done, allow for parameter batch size
        self.loop_using_temp_table_and_sync_koppelingen(batch_size=batch_size)

        # delete the temp table
        self.delete_temp_table_for_sync_bestekkoppelingen()

    def get_all_bestekkoppelingen_by_asset_uuids(self, asset_uuids: [str]) -> Generator[tuple]:
        yield from self.eminfra_importer.get_all_bestekkoppelingen_from_webservice_by_asset_uuids(
            asset_uuids=asset_uuids)

    def update_bestekkoppelingen_by_asset_uuids(self, asset_uuids: [str], bestek_koppelingen_dicts_list: [[dict]]) -> None:
        if len(asset_uuids) == 0:
            return

        delete_query = "DELETE FROM public.bestekkoppelingen WHERE assetUuid IN (VALUES ('" + "'::uuid),('".join(asset_uuids)+"'::uuid));"
        cursor = self.postGIS_connector.connection.cursor()
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

            cursor = self.postGIS_connector.connection.cursor()
            cursor.execute(insert_query)

        self.postGIS_connector.connection.commit()

    def create_temp_table_for_sync_bestekkoppelingen(self):
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
        LEFT JOIN public.assets on assets.assettype = assettypes.uuid
    WHERE bestek = TRUE)
INSERT INTO public.temp_sync_bestekkoppelingen (assetUuid) 
SELECT uuid FROM bestek_assets;"""

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(create_table_query)

        # only fill_table if the number of records in temp table with NULL for done == 0 (empty or done)
        cursor.execute(check_table_query)
        count_not_done = cursor.fetchone()[0]
        if count_not_done > 0:
            return

        cursor.execute(fill_table_query)
        self.postGIS_connector.connection.commit()

    def delete_temp_table_for_sync_bestekkoppelingen(self):
        delete_table_query = "DROP TABLE IF EXISTS public.temp_sync_bestekkoppelingen;"

        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(delete_table_query)
        self.postGIS_connector.connection.commit()

    def loop_using_temp_table_and_sync_koppelingen(self, batch_size: int):
        select_from_temp_table_query = f"SELECT assetUuid FROM public.temp_sync_bestekkoppelingen WHERE done IS NULL LIMIT {batch_size};"
        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(select_from_temp_table_query)
        assets_to_update = list(map(lambda x: x[0], cursor.fetchall()))
        while len(assets_to_update) > 0:
            koppelingen_list = []
            asset_uuid_list = []
            koppelingen_generator = self.get_all_bestekkoppelingen_by_asset_uuids(asset_uuids=assets_to_update)
            for asset_uuid, koppelingen in koppelingen_generator:
                asset_uuid_list.append(asset_uuid)
                koppelingen_list.append(list(koppelingen))
            self.update_bestekkoppelingen_by_asset_uuids(asset_uuids=asset_uuid_list,
                                                         bestek_koppelingen_dicts_list=koppelingen_list)

            update_temp_table_query = "UPDATE public.temp_sync_bestekkoppelingen SET done = TRUE WHERE " \
                                      f"assetUuid IN (VALUES ('" + "'::uuid),('".join(assets_to_update)+"'::uuid));"
            cursor.execute(update_temp_table_query)
            self.postGIS_connector.connection.commit()

            cursor.execute(select_from_temp_table_query)
            assets_to_update = list(map(lambda x: x[0], cursor.fetchall()))
