import logging
import time
from typing import Sequence

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Helpers import turn_list_of_lists_into_string


class WeglocatieGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info('started updating weglocatie')
        start = time.time()

        asset_dicts = self.eminfra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)

        amount = self.process_dicts(connection=connection, asset_uuids=uuids, asset_dicts=asset_dicts)

        end = time.time()
        logging.info(f'updated weglocatie of {amount} asset(s) in {str(round(end - start, 2))} seconds.')

    @classmethod
    def process_dicts(cls, connection, asset_uuids: [str], asset_dicts: [dict]):
        with connection.cursor() as cursor:
            weglocatie_values, amount = cls.create_weglocatie_values_string_from_dicts(
                assets_dicts=asset_dicts)
            cls.delete_weglocatie_records(cursor=cursor, uuids=asset_uuids)
            cls.perform_weglocatie_update_with_values(cursor=cursor, values=weglocatie_values)
            return amount

    @classmethod
    def create_weglocatie_values_string_from_dicts(cls, assets_dicts) -> (Sequence, int):
        counter = 0
        values_array = []
        for asset_dict in assets_dicts:
            if 'wl:Weglocatie.score' not in asset_dict:
                continue

            counter += 1
            record_array = [
                f"'{asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[:36]}'",
                f"'{asset_dict['wl:Weglocatie.geometrie']}'",
                f"'{asset_dict['wl:Weglocatie.score']}'",
                f"'{asset_dict['wl:Weglocatie.bron'][62:]}'",
            ]
            values_array.append(record_array)

        values_string = turn_list_of_lists_into_string(values_array)
        return values_string, counter

    @classmethod
    def perform_weglocatie_update_with_values(cls, cursor, values):
        if values != '':
            insert_query = f"""
            WITH s (assetUuid, geometrie, score, bron) 
                AS (VALUES {values}),
            to_insert AS (
                SELECT assetUuid::uuid AS assetUuid, geometrie, score, bron
                FROM s)        
            INSERT INTO public.weglocaties (assetUuid, geometrie, score, bron) 
            SELECT to_insert.assetUuid, to_insert.geometrie, to_insert.score, to_insert.bron
            FROM to_insert;"""
            cursor.execute(insert_query)

    @classmethod
    def delete_weglocatie_records(cls, cursor, uuids):
        if len(uuids) == 0:
            return

        values = "'" + "','".join(uuids) + "'"
        update_query = f"""DELETE FROM weglocatie_wegsegmenten WHERE assetUuid IN ({values});"""
        cursor.execute(update_query)
        update_query = f"""DELETE FROM weglocatie_aanduidingen WHERE assetUuid IN ({values});"""
        cursor.execute(update_query)
        update_query = f"""DELETE FROM weglocaties WHERE assetUuid IN ({values});"""
        cursor.execute(update_query)
