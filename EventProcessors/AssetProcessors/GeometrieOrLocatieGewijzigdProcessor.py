import logging
import time

from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Helpers import turn_list_of_lists_into_string


class GeometrieOrLocatieGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info('started updating geometrie and locatie')
        start = time.time()

        asset_dicts = self.eminfra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)

        amount, amount2 = self.process_dicts(connection=connection, asset_uuids=uuids, asset_dicts=asset_dicts)

        end = time.time()
        logging.info(f'updated geometrie of {amount} asset(s) and locatie of {amount2} asset(s) in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def process_dicts(connection, asset_uuids: [str], asset_dicts: [dict]):
        with connection.cursor() as cursor:
            geometrie_values, amount = GeometrieOrLocatieGewijzigdProcessor.create_geometrie_values_string_from_dicts(
                assets_dicts=asset_dicts)
            GeometrieOrLocatieGewijzigdProcessor.delete_geometrie_records(cursor=cursor, uuids=asset_uuids)
            GeometrieOrLocatieGewijzigdProcessor.perform_geometrie_update_with_values(cursor=cursor,
                                                                                      values=geometrie_values)
            locatie_insert_values, locatie_update_values, amount2 = GeometrieOrLocatieGewijzigdProcessor.\
                create_locatie_values_string_from_dicts(cursor=cursor, uuids=asset_uuids, assets_dicts=asset_dicts)
            GeometrieOrLocatieGewijzigdProcessor.perform_locatie_update_with_values(
                cursor=cursor, insert_values=locatie_insert_values, update_values=locatie_update_values)
            return amount, amount2

    @staticmethod
    def create_geometrie_values_string_from_dicts(assets_dicts):
        counter = 0
        values_array = []
        for asset_dict in assets_dicts:
            if 'geo:Geometrie.log' not in asset_dict:
                continue

            counter += 1
            for geometrie_dict in asset_dict['geo:Geometrie.log']:
                niveau = geometrie_dict.get('geo:DtcLog.niveau')
                niveau = niveau.replace('https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/', '')
                record_array = [
                    f"'{asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[:36]}'",
                    f"'{niveau}'"]

                bron = None
                if 'geo:DtcLog.bron' in geometrie_dict:
                    bron = geometrie_dict['geo:DtcLog.bron'].replace('https://geo.data.wegenenverkeer.be/id/concept/KlLogBron/', '')
                nauwkeurigheid = geometrie_dict.get('geo:DtcLog.nauwkeurigheid')
                gaVersie = geometrie_dict.get('geo:DtcLog.gaVersie')

                geometrie = None
                if 'geo:DtcLog.geometrie' in geometrie_dict:
                    wkt_dict = geometrie_dict['geo:DtcLog.geometrie']
                    if len(wkt_dict.values()) > 1:
                        raise NotImplementedError(f'geo:DtcLog.geometrie in dict of asset {uuid} has more than 1 geometry')

                    if len(wkt_dict.values()) == 1:
                        geometrie = list(wkt_dict.values())[0]

                overerving = None
                if 'geo:DtcLog.overerving' in geometrie_dict and len(geometrie_dict['geo:DtcLog.overerving']) > 0:
                    erflaten = []
                    for overerving in geometrie_dict['geo:DtcLog.overerving']:
                        erflaten.append(overerving['geo:DtcOvererving.erflaatId']['DtcIdentificator.identificator'])
                    overerving = '|'.join(erflaten)

                for value in [gaVersie, nauwkeurigheid, bron, geometrie, overerving]:
                    if value is None or value == '':
                        record_array.append('NULL')
                    else:
                        value = value.replace("'", "''")
                        record_array.append(f"'{value}'")

                if geometrie is not None and geometrie != '':
                    record_array.append(f"ST_GeomFromText('{geometrie}', 31370)")
                else:
                    record_array.append('NULL')

                values_array.append(record_array)

        values_string = turn_list_of_lists_into_string(values_array)
        return values_string, counter

    @staticmethod
    def perform_geometrie_update_with_values(cursor, values):
        if values != '':
            insert_query = f"""
            WITH s (assetUuid, geo_niveau, ga_versie, nauwkeurigheid, bron, wkt_string, overerving_ids, geometry) 
                AS (VALUES {values}),
            to_insert AS (
                SELECT assetUuid::uuid AS assetUuid, geo_niveau::integer, ga_versie, nauwkeurigheid, bron, wkt_string, 
                    overerving_ids, geometry
                FROM s)        
            INSERT INTO public.geometrie (assetUuid, geo_niveau, ga_versie, nauwkeurigheid, bron, wkt_string, 
                overerving_ids, geometry) 
            SELECT to_insert.assetUuid, to_insert.geo_niveau, to_insert.ga_versie, to_insert.nauwkeurigheid, 
                to_insert.bron, to_insert.wkt_string, to_insert.overerving_ids, to_insert.geometry
            FROM to_insert;"""
            cursor.execute(insert_query)

    @staticmethod
    def create_locatie_values_string_from_dicts(cursor, uuids: [str], assets_dicts: [dict]):
        all_asset_uuids = set(uuids)
        values_list_all_asset_uuids = "','".join(all_asset_uuids)
        select_query = f"""
                SELECT assetUuid FROM public.locatie WHERE assetUuid IN ('{values_list_all_asset_uuids}');"""
        cursor.execute(select_query)

        assets_to_update = set(map(lambda a: a[0], cursor.fetchall()))

        insert_values_array = []
        update_values_array = []

        counter = 0
        for asset_dict in assets_dicts:
            counter += 1
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[:36]
            record_array = [f"'{uuid}'"]

            omschrijving = asset_dict.get('loc:Locatie.omschrijving')
            geometrie = asset_dict.get('loc:Locatie.geometrie')
            locatie_dict = asset_dict.get('loc:Locatie.puntlocatie')

            if locatie_dict is not None and locatie_dict != '':
                bron = None
                if 'loc:DtcPuntlocatie.bron' in locatie_dict:
                    bron = locatie_dict['loc:DtcPuntlocatie.bron'].replace(
                        'https://loc.data.wegenenverkeer.be/id/concept/KlLocatieBron/', '')
                precisie = None
                if 'loc:DtcPuntlocatie.precisie' in locatie_dict:
                    precisie = locatie_dict['loc:DtcPuntlocatie.precisie'].replace(
                        'https://loc.data.wegenenverkeer.be/id/concept/KlLocatiePrecisie/', '')

                ident8 = ident2 = referentiepaal_opschrift = referentiepaal_afstand = straatnaam = weg_gemeente = None

                punt_dict = locatie_dict.get('loc:3Dpunt.puntgeometrie')
                if punt_dict is not None and punt_dict != '':
                    x = punt_dict['loc:DtcCoord.lambert72']['loc:DtcCoordLambert72.xcoordinaat']
                    y = punt_dict['loc:DtcCoord.lambert72']['loc:DtcCoordLambert72.ycoordinaat']
                    z = punt_dict['loc:DtcCoord.lambert72']['loc:DtcCoordLambert72.zcoordinaat']
                else:
                    x = y = z = None

                adres_dict = locatie_dict.get('loc:DtcPuntlocatie.adres')
                if adres_dict is not None and adres_dict != '':
                    straat = adres_dict['loc:DtcAdres.straat']
                    nummer = adres_dict['loc:DtcAdres.nummer']
                    bus = adres_dict['loc:DtcAdres.bus']
                    postcode = adres_dict['loc:DtcAdres.postcode']
                    gemeente = adres_dict['loc:DtcAdres.gemeente']
                    provincie = adres_dict['loc:DtcAdres.provincie']
                else:
                    straat = nummer = bus = postcode = gemeente = provincie = None

                weglocatie_dict = locatie_dict.get('loc:DtcPuntlocatie.weglocatie')
                if weglocatie_dict is not None and weglocatie_dict != '':
                    ident8 = weglocatie_dict['loc:DtcWeglocatie.ident8']
                    ident2 = weglocatie_dict['loc:DtcWeglocatie.ident2']
                    referentiepaal_opschrift = weglocatie_dict['loc:DtcWeglocatie.referentiepaalOpschrift']
                    referentiepaal_afstand = weglocatie_dict['loc:DtcWeglocatie.referentiepaalAfstand']
                    straatnaam = weglocatie_dict['loc:DtcWeglocatie.straatnaam']
                    weg_gemeente = weglocatie_dict['loc:DtcWeglocatie.gemeente']

                for value in [omschrijving, geometrie, bron, precisie]:
                    if value is None or value == '':
                        record_array.append("NULL")
                    else:
                        value = value.replace("'", "''")
                        record_array.append(f"'{value}'")
                for value in [x, y, z]:
                    if value is None or value == '':
                        record_array.append("NULL")
                    else:
                        record_array.append(f"{value}")
                for value in [straat, nummer, bus, postcode, gemeente, provincie, ident8, ident2]:
                    if value is None or value == '':
                        record_array.append("NULL")
                    else:
                        value = value.replace("'", "''")
                        record_array.append(f"'{value}'")
                for value in [referentiepaal_opschrift, referentiepaal_afstand]:
                    if value is None or value == '':
                        record_array.append("NULL")
                    else:
                        record_array.append(f"{value}")
                for value in [straatnaam, weg_gemeente]:
                    if value is None or value == '':
                        record_array.append("NULL")
                    else:
                        value = value.replace("'", "''")
                        record_array.append(f"'{value}'")
                if geometrie is not None and geometrie != '':
                    record_array.append(f"ST_GeomFromText('{geometrie}', 31370)")
                else:
                    record_array.append('NULL')
            else:
                for value in [omschrijving, geometrie]:
                    if value is None or value == '':
                        record_array.append("NULL")
                    else:
                        value = value.replace("'", "''")
                        record_array.append(f"'{value}'")
                for _ in range(17):
                    record_array.append("NULL")
                if geometrie is not None and geometrie != '':
                    record_array.append(f"ST_GeomFromText('{geometrie}', 31370)")
                else:
                    record_array.append('NULL')

            if uuid not in assets_to_update:
                insert_values_array.append(record_array)
            else:
                update_values_array.append(record_array)

        insert_values = turn_list_of_lists_into_string(insert_values_array)
        update_values = turn_list_of_lists_into_string(update_values_array)
        return insert_values, update_values, counter

    @staticmethod
    def perform_locatie_update_with_values(cursor, insert_values, update_values):
        if insert_values != '':
            insert_query = f"""
                WITH s (assetUuid, omschrijving, geometrie, bron, precisie, x, y, z, straat, nummer, bus, postcode, 
                    gemeente, provincie, ident8, ident2, referentiepaal_opschrift, referentiepaal_afstand, straatnaam, 
                    weg_gemeente, geometry) 
                    AS (VALUES {insert_values}),
                to_insert AS (
                    SELECT assetUuid::uuid AS assetUuid, omschrijving, geometrie, bron, precisie, x::decimal as x, 
                        y::decimal as y, z::decimal as z, straat, nummer, bus, postcode, gemeente, provincie, ident8, 
                        ident2, referentiepaal_opschrift::decimal as referentiepaal_opschrift, 
                        referentiepaal_afstand::integer as referentiepaal_afstand, straatnaam, weg_gemeente, geometry
                    FROM s)        
                INSERT INTO public.locatie (assetUuid, omschrijving, geometrie, bron, precisie, x, y, z, adres_straat, 
                    adres_nummer, adres_bus, adres_postcode, adres_gemeente, adres_provincie, ident8, ident2, 
                    referentiepaal_opschrift, referentiepaal_afstand, straatnaam, gemeente, geometry) 
                SELECT to_insert.assetUuid, to_insert.omschrijving, to_insert.geometrie, to_insert.bron, to_insert.precisie,
                    to_insert.x, to_insert.y, to_insert.z, to_insert.straat, to_insert.nummer, to_insert.bus, 
                    to_insert.postcode, to_insert.gemeente, to_insert.provincie, to_insert.ident8, to_insert.ident2, 
                    to_insert.referentiepaal_opschrift, to_insert.referentiepaal_afstand, to_insert.straatnaam, 
                    to_insert.weg_gemeente, to_insert.geometry
                FROM to_insert;"""
            cursor.execute(insert_query)

        if update_values != '':
            update_query = f"""
                WITH s (assetUuid, omschrijving, geometrie, bron, precisie, x, y, z, straat, nummer, bus, postcode, 
                    gemeente, provincie, ident8, ident2, referentiepaal_opschrift, referentiepaal_afstand, straatnaam, 
                    weg_gemeente, geometry) 
                    AS (VALUES {update_values}),
                to_update AS (
                    SELECT assetUuid::uuid AS assetUuid, omschrijving, geometrie, bron, precisie, x::decimal as x, 
                        y::decimal as y, z::decimal as z, straat, nummer, bus, postcode, gemeente, provincie, ident8, 
                        ident2, referentiepaal_opschrift::decimal as referentiepaal_opschrift, 
                        referentiepaal_afstand::integer as referentiepaal_afstand, straatnaam, weg_gemeente, geometry
                    FROM s)       
                UPDATE locatie 
                SET omschrijving = to_update.omschrijving, geometrie = to_update.geometrie, bron = to_update.bron, 
                    precisie = to_update.precisie, x = to_update.x, y = to_update.y, z = to_update.z, 
                    adres_straat = to_update.straat, adres_nummer = to_update.nummer, adres_bus = to_update.bus, 
                    adres_postcode = to_update.postcode, adres_gemeente = to_update.gemeente, 
                    adres_provincie = to_update.provincie, ident8 = to_update.ident8, ident2 = to_update.ident2, 
                    referentiepaal_opschrift = to_update.referentiepaal_opschrift, 
                    referentiepaal_afstand = to_update.referentiepaal_afstand, 
                    straatnaam = to_update.straatnaam, gemeente = to_update.weg_gemeente, geometry = to_update.geometry
                FROM to_update
                WHERE to_update.assetUuid = locatie.assetUuid;"""
            cursor.execute(update_query)

    @staticmethod
    def delete_geometrie_records(cursor, uuids):
        if len(uuids) == 0:
            return

        values = "'" + "','".join(uuids) + "'"
        update_query = f"""DELETE FROM geometrie WHERE assetUuid IN ({values})"""
        cursor.execute(update_query)
