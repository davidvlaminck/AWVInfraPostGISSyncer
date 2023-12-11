import logging
import time

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Helpers import turn_list_of_lists_into_string


class WeglocatieGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started updating weglocatie')
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
    def create_weglocatie_values_string_from_dicts(cls, assets_dicts):
        counter = 0
        values_array = []
        for asset_dict in assets_dicts:
            if 'geo:weglocatie.log' not in asset_dict:
                continue

            counter += 1
            for weglocatie_dict in asset_dict['geo:weglocatie.log']:
                niveau = weglocatie_dict.get('geo:DtcLog.niveau')
                niveau = niveau.replace('https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/', '')
                record_array = [f"'{asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]}'",
                                f"'{niveau}'"]

                bron = None
                if 'geo:DtcLog.bron' in weglocatie_dict:
                    bron = weglocatie_dict['geo:DtcLog.bron'].replace('https://geo.data.wegenenverkeer.be/id/concept/KlLogBron/', '')
                nauwkeurigheid = weglocatie_dict.get('geo:DtcLog.nauwkeurigheid')
                gaVersie = weglocatie_dict.get('geo:DtcLog.gaVersie')

                weglocatie = None
                if 'geo:DtcLog.weglocatie' in weglocatie_dict:
                    wkt_dict = weglocatie_dict['geo:DtcLog.weglocatie']
                    if len(wkt_dict.values()) > 1:
                        raise NotImplementedError(f'geo:DtcLog.weglocatie in dict of asset {uuid} has more than 1 geometry')

                    if len(wkt_dict.values()) == 1:
                        weglocatie = list(wkt_dict.values())[0]

                overerving = None
                if 'geo:DtcLog.overerving' in weglocatie_dict and len(weglocatie_dict['geo:DtcLog.overerving']) > 0:
                    erflaten = []
                    for overerving in weglocatie_dict['geo:DtcLog.overerving']:
                        erflaten.append(overerving['geo:DtcOvererving.erflaatId']['DtcIdentificator.identificator'])
                    overerving = '|'.join(erflaten)

                for value in [gaVersie, nauwkeurigheid, bron, weglocatie, overerving]:
                    if value is None or value == '':
                        record_array.append('NULL')
                    else:
                        value = value.replace("'", "''")
                        record_array.append(f"'{value}'")

                values_array.append(record_array)

        values_string = turn_list_of_lists_into_string(values_array)
        return values_string, counter

    @classmethod
    def perform_weglocatie_update_with_values(cls, cursor, values):
        if values != '':
            insert_query = f"""
            WITH s (assetUuid, geo_niveau, ga_versie, nauwkeurigheid, bron, wkt_string, overerving_ids) 
                AS (VALUES {values}),
            to_insert AS (
                SELECT assetUuid::uuid AS assetUuid, geo_niveau::integer, ga_versie, nauwkeurigheid, bron, wkt_string, overerving_ids
                FROM s)        
            INSERT INTO public.weglocatie (assetUuid, geo_niveau, ga_versie, nauwkeurigheid, bron, wkt_string, overerving_ids) 
            SELECT to_insert.assetUuid, to_insert.geo_niveau, to_insert.ga_versie, to_insert.nauwkeurigheid, 
                to_insert.bron, to_insert.wkt_string, to_insert.overerving_ids
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

