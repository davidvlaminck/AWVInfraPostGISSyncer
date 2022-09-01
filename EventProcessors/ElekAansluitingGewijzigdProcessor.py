import logging
import time

from EventProcessors.SpecificEventProcessor import SpecificEventProcessor


class ElekAansluitingGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, em_infra_importer):
        super().__init__(cursor, em_infra_importer)

    def process(self, uuids: [str]):
        logging.info(f'started updating elek aansluiting')
        start = time.time()

        aansluitingen_generator = self.em_infra_importer.get_all_elek_aansluitingen_from_webservice_by_asset_uuids(
            asset_uuids=uuids)
        for asset_uuid, aansluiting_dict in aansluitingen_generator:

            self.update_aansluiting_by_asset_uuid(cursor=self.cursor, asset_uuid=asset_uuid,
                                                  aansluiting_dict=list(aansluiting_dict))

        end = time.time()
        logging.info(f'updated elek aansluting for up to {len(uuids)} assets in {str(round(end - start, 2))} seconds.')

    def update_aansluiting_by_asset_uuid(self, cursor, asset_uuid, aansluiting_dict):
        delete_query = f"DELETE FROM public.elek_aansluitingen WHERE assetUuid = '{asset_uuid}'"
        cursor.execute(delete_query)
        if 'elektriciteitsAansluitingRef' not in aansluiting_dict[0]:
            return
        single_aansluiting_dict = aansluiting_dict[0]['elektriciteitsAansluitingRef']
        ean = single_aansluiting_dict['ean']
        aansluitnummer = single_aansluiting_dict['aansluitnummer']
        insert_query = f"""INSERT INTO elek_aansluitingen (assetUuid, EAN, aansluiting) 
        VALUES ('{asset_uuid}','{ean}','{aansluitnummer}')"""
        cursor.execute(insert_query)

    @staticmethod
    def create_geometrie_values_string_from_dicts(assets_dicts):
        values = ''
        for asset_dict in assets_dicts:
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]

            if 'geo:Geometrie.log' not in asset_dict:
                continue

            for geometrie_dict in asset_dict['geo:Geometrie.log']:
                niveau = geometrie_dict.get('geo:DtcLog.niveau')
                niveau = niveau.replace('https://geo.data.wegenenverkeer.be/id/concept/KlLogNiveau/', '')
                values += f"('{uuid}',{niveau},"

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
                        values += 'NULL,'
                    else:
                        value = value.replace("'", "''")
                        values += f"'{value}',"
                values = values[:-1] + '),'
        return values

    @staticmethod
    def perform_geometrie_update_with_values(cursor, values):
        insert_query = f"""
        WITH s (assetUuid, niveau, ga_versie, nauwkeurigheid, bron, wkt_string, overerving_ids) 
            AS (VALUES {values[:-1]}),
        to_insert AS (
            SELECT assetUuid::uuid AS assetUuid, niveau::integer, ga_versie, nauwkeurigheid, bron, wkt_string, overerving_ids
            FROM s)        
        INSERT INTO public.geometrie (assetUuid, niveau, ga_versie, nauwkeurigheid, bron, wkt_string, overerving_ids) 
        SELECT to_insert.assetUuid, to_insert.niveau, to_insert.ga_versie, to_insert.nauwkeurigheid, 
            to_insert.bron, to_insert.wkt_string, to_insert.overerving_ids
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

        insert_values = ''
        update_values = ''
        for asset_dict in assets_dicts:
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]

            values = f"('{uuid}',"

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
                        values += 'NULL,'
                    else:
                        value = value.replace("'", "''")
                        values += f"'{value}',"
                for value in [x, y, z]:
                    if value is None or value == '':
                        values += 'NULL,'
                    else:
                        values += f"{value},"
                for value in [straat, nummer, bus, postcode, gemeente, provincie, ident8, ident2]:
                    if value is None or value == '':
                        values += 'NULL,'
                    else:
                        value = value.replace("'", "''")
                        values += f"'{value}',"
                for value in [referentiepaal_opschrift, referentiepaal_afstand]:
                    if value is None or value == '':
                        values += 'NULL,'
                    else:
                        values += f"{value},"
                for value in [straatnaam, weg_gemeente]:
                    if value is None or value == '':
                        values += 'NULL,'
                    else:
                        value = value.replace("'", "''")
                        values += f"'{value}',"
                values = values[:-1] + '),'
                if uuid not in assets_to_update:
                    insert_values += values
                else:
                    update_values += values
            else:
                for value in [omschrijving, geometrie]:
                    if value is None or value == '':
                        values += 'NULL,'
                    else:
                        value = value.replace("'", "''")
                        values += f"'{value}',"
                values = values + ' NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),'
                if uuid not in assets_to_update:
                    insert_values += values
                else:
                    update_values += values

        return insert_values[:-1], update_values[:-1]

    @staticmethod
    def perform_locatie_update_with_values(cursor, insert_values, update_values):
        if insert_values != '':
            insert_query = f"""
                WITH s (assetUuid, omschrijving, geometrie, bron, precisie, x, y, z, straat, nummer, bus, postcode, 
                    gemeente, provincie, ident8, ident2, referentiepaal_opschrift, referentiepaal_afstand, straatnaam, 
                    weg_gemeente) 
                    AS (VALUES {insert_values}),
                to_insert AS (
                    SELECT assetUuid::uuid AS assetUuid, omschrijving, geometrie, bron, precisie, x::decimal as x, 
                        y::decimal as y, z::decimal as z, straat, nummer, bus, postcode, gemeente, provincie, ident8, 
                        ident2, referentiepaal_opschrift::decimal as referentiepaal_opschrift, 
                        referentiepaal_afstand::integer as referentiepaal_afstand, straatnaam, weg_gemeente
                    FROM s)        
                INSERT INTO public.locatie (assetUuid, omschrijving, geometrie, bron, precisie, x, y, z, adres_straat, 
                    adres_nummer, adres_bus, adres_postcode, adres_gemeente, adres_provincie, ident8, ident2, 
                    referentiepaal_opschrift, referentiepaal_afstand, straatnaam, gemeente) 
                SELECT to_insert.assetUuid, to_insert.omschrijving, to_insert.geometrie, to_insert.bron, to_insert.precisie,
                    to_insert.x, to_insert.y, to_insert.z, to_insert.straat, to_insert.nummer, to_insert.bus, 
                    to_insert.postcode, to_insert.gemeente, to_insert.provincie, to_insert.ident8, to_insert.ident2, 
                    to_insert.referentiepaal_opschrift, to_insert.referentiepaal_afstand, to_insert.straatnaam, 
                    to_insert.weg_gemeente
                FROM to_insert;"""
            cursor.execute(insert_query)

        if update_values != '':
            update_query = f"""
                WITH s (assetUuid, omschrijving, geometrie, bron, precisie, x, y, z, straat, nummer, bus, postcode, 
                    gemeente, provincie, ident8, ident2, referentiepaal_opschrift, referentiepaal_afstand, straatnaam, 
                    weg_gemeente) 
                    AS (VALUES {update_values}),
                to_update AS (
                    SELECT assetUuid::uuid AS assetUuid, omschrijving, geometrie, bron, precisie, x::decimal as x, 
                        y::decimal as y, z::decimal as z, straat, nummer, bus, postcode, gemeente, provincie, ident8, 
                        ident2, referentiepaal_opschrift::decimal as referentiepaal_opschrift, 
                        referentiepaal_afstand::integer as referentiepaal_afstand, straatnaam, weg_gemeente
                    FROM s)       
                UPDATE locatie 
                SET omschrijving = to_update.omschrijving, geometrie = to_update.geometrie, bron = to_update.bron, 
                    precisie = to_update.precisie, x = to_update.x, y = to_update.y, z = to_update.z, 
                    adres_straat = to_update.straat, adres_nummer = to_update.nummer, adres_bus = to_update.bus, 
                    adres_postcode = to_update.postcode, adres_gemeente = to_update.gemeente, 
                    adres_provincie = to_update.provincie, ident8 = to_update.ident8, ident2 = to_update.ident2, 
                    referentiepaal_opschrift = to_update.referentiepaal_opschrift, 
                    referentiepaal_afstand = to_update.referentiepaal_afstand, 
                    straatnaam = to_update.straatnaam, gemeente = to_update.weg_gemeente
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
