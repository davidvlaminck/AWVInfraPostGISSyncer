import logging
import time

import psycopg2

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Exceptions.BestekMissingError import BestekMissingError


class BestekGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info('started updating bestekkoppelingen')
        start = time.time()

        asset_dicts_dict = {}
        for asset_dict in list(self.eminfra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)):
            asset_uuid = asset_dict['@id'].split('/')[-1][:36]
            asset_dicts_dict[asset_uuid] = asset_dict

        amount = self.update_bestekkoppelingen(connection=connection,
                                               asset_dicts_dict=asset_dicts_dict)

        end = time.time()
        logging.info(f'updated bestekkoppelingen of {amount} asset(s) in {str(round(end - start, 2))} seconds.')

    @classmethod
    def update_bestekkoppelingen(cls, connection, asset_dicts_dict: dict[str, dict]) -> int:
        if not asset_dicts_dict:
            return 0

        asset_uuids = list(asset_dicts_dict.keys())
        delete_query = "DELETE FROM public.bestekkoppelingen WHERE assetUuid IN (VALUES ('" + "'::uuid),('".join(
            asset_uuids) + "'::uuid));"
        with connection.cursor() as cursor:
            cursor.execute(delete_query)

            for asset_uuid, asset_dict in asset_dicts_dict.items():
                values = ''
                bestek_koppelingen_list = asset_dict.get('bs:Bestek.bestekkoppeling')
                if bestek_koppelingen_list is None or not bestek_koppelingen_list:
                    continue

                for bestek_koppeling_dict in bestek_koppelingen_list:
                    bestek_uuid = bestek_koppeling_dict[
                        'bs:DtcBestekkoppeling.bestekId']['DtcIdentificator.identificator'][:36]
                    start_datum = bestek_koppeling_dict['bs:DtcBestekkoppeling.actiefVan']
                    eind_datum = bestek_koppeling_dict.get('bs:DtcBestekkoppeling.actiefTot', '')

                    koppeling_status = bestek_koppeling_dict['bs:DtcBestekkoppeling.status'].split('/')[-1]

                    values += f"('{asset_uuid}','{bestek_uuid}',"
                    values += 'NULL,' if start_datum == '' else f"'{start_datum}',"
                    values += 'NULL,' if eind_datum == '' else f"'{eind_datum}',"
                    values += f"'{koppeling_status}'),"

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
                        raise BestekMissingError() from exc
                    else:
                        raise exc

        return len(asset_uuids)
