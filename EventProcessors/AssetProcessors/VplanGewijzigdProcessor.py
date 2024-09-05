import logging
import time

import psycopg2

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Exceptions.AssetMissingError import AssetMissingError
from Helpers import turn_list_of_lists_into_string


class VplanGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started updating vplan')
        start = time.time()

        vplannen_generator = self.eminfra_importer.get_all_vplankoppelingen_from_webservice_by_asset_uuids(
            asset_uuids=uuids)
        for asset_uuid, vplankoppeling_list in vplannen_generator:
            self.update_vplankoppelingen_by_asset_uuid(connection=connection, asset_uuid=asset_uuid,
                                                       vplankoppeling_list=list(vplankoppeling_list))

        end = time.time()
        logging.info(f'updated vplan for up to {len(uuids)} assets in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def update_vplankoppelingen_by_asset_uuid(connection, asset_uuid, vplankoppeling_list):
        delete_query = f"DELETE FROM public.vplan_koppelingen WHERE assetUuid = '{asset_uuid}';"
        with connection.cursor() as cursor:
            cursor.execute(delete_query)
            if len(vplankoppeling_list) == 0 or 'vplanRef' not in vplankoppeling_list[0]:
                return

            values_array = []
            for vplan_koppeling in vplankoppeling_list:
                vplannr = vplan_koppeling['vplanRef']['nummer']
                if vplannr is not None:
                    vplannr = vplannr.replace("'", "''")
                record_array = [f"'{vplan_koppeling['uuid']}'", f"'{asset_uuid}'",
                                f"'{vplannr}'", f"'{vplan_koppeling['vplanRef']['uuid']}'"]

                in_dienst_datum = vplan_koppeling.get('inDienstDatum', None)
                uit_dienst_datum = vplan_koppeling.get('uitDienstDatum', None)
                commentaar = vplan_koppeling.get('commentaar', None)
                if commentaar is not None:
                    commentaar = commentaar.replace("'", "''")
                for field in [in_dienst_datum, uit_dienst_datum, commentaar]:
                    if field is None:
                        record_array.append('NULL')
                    else:
                        record_array.append(f"'{field}'")

                values_array.append(record_array)

            values_string = turn_list_of_lists_into_string(values_array)

            insert_query = f"""
WITH s (uuid, assetUuid, vplannummer, vplan, inDienstDatum, uitDienstDatum, commentaar) 
    AS (VALUES {values_string}),
to_insert AS (
    SELECT uuid::uuid AS uuid, assetUuid::uuid AS assetUuid, vplannummer, vplan::uuid as vplan, 
    inDienstDatum::TIMESTAMP as inDienstDatum, uitDienstDatum::TIMESTAMP as uitDienstDatum, commentaar
    FROM s)
INSERT INTO public.vplan_koppelingen (uuid, assetUuid, vplannummer, vplan, inDienstDatum, uitDienstDatum, commentaar)
SELECT to_insert.uuid, to_insert.assetUuid, to_insert.vplannummer, to_insert.vplan, to_insert.inDienstDatum, 
    to_insert.uitDienstDatum, to_insert.commentaar
FROM to_insert;"""

            try:
                cursor.execute(insert_query)
            except psycopg2.Error as exc:
                if str(exc).split('\n')[0] == 'insert or update on table "vplan_koppelingen" violates foreign key ' \
                                              'constraint "assets_vplan_koppelingen_fkey"':
                    connection.rollback()
                    raise AssetMissingError()
                else:
                    connection.rollback()
                    raise exc
