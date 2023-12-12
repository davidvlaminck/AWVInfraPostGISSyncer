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
        with (connection.cursor() as cursor):
            weglocatie_values, amount, wegsegmenten_values, wegaanduidingen_values = cls.create_weglocatie_values_string_from_dicts(
                assets_dicts=asset_dicts)
            cls.delete_weglocatie_records(cursor=cursor, uuids=asset_uuids)
            cls.perform_weglocatie_update_with_values(cursor=cursor, values=weglocatie_values)
            cls.perform_wegsegmenten_update_with_values(cursor=cursor, values=wegsegmenten_values)
            cls.perform_wegaanduidingen_update_with_values(cursor=cursor, values=wegaanduidingen_values)
            return amount

    @classmethod
    def create_weglocatie_values_string_from_dicts(cls, assets_dicts) -> (Sequence, int, Sequence, Sequence):
        counter = 0
        wl_values_array = []
        wegsegmenten_values_array = []
        wegaanduidingen_values_array = []
        for asset_dict in assets_dicts:
            if 'wl:Weglocatie.score' not in asset_dict:
                continue
            asset_uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[:36]

            counter += 1
            record_array = [
                f"'{asset_uuid}'",
                f"'{asset_dict['wl:Weglocatie.geometrie']}'",
                f"'{asset_dict['wl:Weglocatie.score']}'",
                f"'{asset_dict['wl:Weglocatie.bron'][62:]}'",
            ]
            wl_values_array.append(record_array)

            if 'wl:Weglocatie.wegsegment' in asset_dict:
                wegsegmenten_values_array.extend(
                    [f"'{asset_uuid}'", f"{wegsegment['wl:DtcWegsegment.oidn']}"]
                    for wegsegment in asset_dict['wl:Weglocatie.wegsegment']
                )

            if 'wl:Weglocatie.wegaanduiding' in asset_dict:
                record_array = []
                for wegaanduiding in asset_dict['wl:Weglocatie.wegaanduiding']:
                    record_array.extend([
                        f"'{asset_uuid}'",
                        f"'{wegaanduiding['wl:DtcWegaanduiding.weg']['wl:DtcWeg.nummer']}'",
                        f"'{wegaanduiding['wl:DtcWegaanduiding.van']['wl:DtcRelatieveLocatie.weg']['wl:DtcWeg.nummer']}'",
                        f"'{wegaanduiding['wl:DtcWegaanduiding.van']['wl:DtcRelatieveLocatie.referentiepunt']['wl:DtcReferentiepunt.weg']['wl:DtcWeg.nummer']}'",
                        f"'{wegaanduiding['wl:DtcWegaanduiding.van']['wl:DtcRelatieveLocatie.referentiepunt']['wl:DtcReferentiepunt.opschrift']}'",
                        f"{wegaanduiding['wl:DtcWegaanduiding.van']['wl:DtcRelatieveLocatie.afstand']}",
                        f"'{wegaanduiding['wl:DtcWegaanduiding.tot']['wl:DtcRelatieveLocatie.weg']['wl:DtcWeg.nummer']}'",
                        f"'{wegaanduiding['wl:DtcWegaanduiding.tot']['wl:DtcRelatieveLocatie.referentiepunt']['wl:DtcReferentiepunt.weg']['wl:DtcWeg.nummer']}'",
                        f"'{wegaanduiding['wl:DtcWegaanduiding.tot']['wl:DtcRelatieveLocatie.referentiepunt']['wl:DtcReferentiepunt.opschrift']}'",
                        f"{wegaanduiding['wl:DtcWegaanduiding.tot']['wl:DtcRelatieveLocatie.afstand']}",
                    ])
                wegaanduidingen_values_array.append(record_array)

        wl_values_string = turn_list_of_lists_into_string(wl_values_array)
        wegsegmenten_values_string = turn_list_of_lists_into_string(wegsegmenten_values_array)
        wegaanduidingen_values_string = turn_list_of_lists_into_string(wegaanduidingen_values_array)
        return wl_values_string, counter, wegsegmenten_values_string, wegaanduidingen_values_string

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
    def perform_wegsegmenten_update_with_values(cls, cursor, values):
        if values != '':
            insert_query = f"""
            WITH s (assetUuid, oidn) 
                AS (VALUES {values}),
            to_insert AS (
                SELECT assetUuid::uuid AS assetUuid, oidn
                FROM s)        
            INSERT INTO public.weglocatie_wegsegmenten (assetUuid, oidn) 
            SELECT to_insert.assetUuid, to_insert.oidn
            FROM to_insert;"""
            cursor.execute(insert_query)

    @classmethod
    def perform_wegaanduidingen_update_with_values(cls, cursor, values):
        if values != '':
            insert_query = f"""
            WITH s (assetUuid, wegnummer, van_wegnummer, van_ref_wegnummer, van_ref_opschrift, van_afstand, 
                tot_wegnummer, tot_ref_wegnummer, tot_ref_opschrift, tot_afstand) 
                AS (VALUES {values}),
            to_insert AS (
                SELECT assetUuid::uuid AS assetUuid, wegnummer, van_wegnummer, van_ref_wegnummer, van_ref_opschrift, van_afstand, 
                tot_wegnummer, tot_ref_wegnummer, tot_ref_opschrift, tot_afstand
                FROM s)        
            INSERT INTO public.weglocatie_aanduidingen (assetUuid, wegnummer, van_wegnummer, van_ref_wegnummer, van_ref_opschrift, van_afstand, 
                tot_wegnummer, tot_ref_wegnummer, tot_ref_opschrift, tot_afstand) 
            SELECT to_insert.assetUuid, to_insert.wegnummer, to_insert.van_wegnummer, to_insert.van_ref_wegnummer, 
                to_insert.van_ref_opschrift, to_insert.van_afstand, to_insert.tot_wegnummer, to_insert.tot_ref_wegnummer, 
                to_insert.tot_ref_opschrift, to_insert.tot_afstand
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
