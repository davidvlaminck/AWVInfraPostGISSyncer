import json
from collections.abc import Generator

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class BestekKoppelingSyncer:
    def __init__(self, postGIS_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postGIS_connector = postGIS_connector
        self.eminfra_importer = em_infra_importer

    def sync_bestekkoppelingen(self):
        self.update_all_bestekkoppelingen()
        self.postGIS_connector.connection.commit()

    def update_all_bestekkoppelingen(self):
        # create a temp table that holds all asset_uuid

        # go through all of the table and flag as sync'd when done, allow for parameter batch size
        # delete the temp table

        bestekkoppelingen = self.get_all_bestekkoppelingen_by_asset_uuids(asset_uuids=[])
        self.update_bestekkoppelingen(bestek_koppelingen_dicts=bestekkoppelingen)

    def get_all_bestekkoppelingen_by_asset_uuids(self, asset_uuids: [str]) -> Generator[dict]:
        yield from self.eminfra_importer.import_all_bestekkoppelingen_from_webservice_by_asset_uuids(
            asset_uuids=asset_uuids)

    def update_bestekkoppelingen_by_asset_uuids(self, asset_uuids: [str], bestek_koppelingen_dicts: [dict]):
        if len(asset_uuids) == 0:
            return

        delete_query = "DELETE FROM public.bestekkoppelingen WHERE assetUuid IN (VALUES ('" + "'::uuid),('".join(asset_uuids)+"'::uuid));"
        cursor = self.postGIS_connector.connection.cursor()
        cursor.execute(delete_query)

        for asset_uuid in asset_uuids:
            if len(bestek_koppelingen_dicts) == 0:
                continue

            values = ''
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
