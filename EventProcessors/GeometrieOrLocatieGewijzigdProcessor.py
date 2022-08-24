import logging
import time

from EventProcessors.SpecificEventProcessor import SpecificEventProcessor


class GeometrieOrLocatieGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, em_infra_importer):
        super().__init__(cursor, em_infra_importer)

    def process(self, uuids: [str]):
        logging.info(f'started changing geometrie and locatie')
        start = time.time()

        asset_dicts = self.em_infra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        geometrie_values = self.create_geometrie_values_string_from_dicts(assets_dicts=asset_dicts)
        self.delete_geometrie_records(cursor=self.cursor, uuids=uuids)
        self.perform_geometrie_update_with_values(cursor=self.cursor, values=geometrie_values)

        end = time.time()
        logging.info(f'updated {len(asset_dicts)} assets in {str(round(end - start, 2))} seconds.')

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
                    if value is None:
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

    # def process_dicts(self, assetDicts):
    #     asset_processor = NieuwAssetProcessor()
    #     logging.info(f'started changing geometrie/locatie of {len(assetDicts)} assets')
    #     for asset_dict in assetDicts:
    #         flattened_dict = asset_processor.flatten_dict(input_dict=asset_dict)
    #
    #         korte_uri = flattened_dict['typeURI'].split('/ns/')[1]
    #         ns = korte_uri.split('#')[0]
    #         assettype = korte_uri.split('#')[1]
    #         if '-' in assettype:
    #             assettype = '`' + assettype + '`'
    #
    #         flattened_dict["geometry"] = asset_processor.get_wkt_from_puntlocatie(flattened_dict)
    #         if 'loc:geometrie' in flattened_dict.keys():
    #             geometrie = flattened_dict['loc:geometrie']
    #             if geometrie != '' and flattened_dict["geometry"] == '':
    #                 flattened_dict["geometry"] = geometrie
    #
    #         locatie_attributen = ['geometry', 'loc:geometrie', 'loc:omschrijving', 'loc:puntlocatie.loc:adres.loc:bus',
    #                               'loc:puntlocatie.loc:adres.loc:gemeente', 'loc:puntlocatie.loc:adres.loc:nummer',
    #                               'loc:puntlocatie.loc:adres.loc:postcode', 'loc:puntlocatie.loc:adres.loc:provincie',
    #                               'loc:puntlocatie.loc:adres.loc:straat', 'loc:puntlocatie.loc:bron',
    #                               'loc:puntlocatie.loc:precisie',
    #                               'loc:puntlocatie.loc:puntgeometrie.loc:lambert72.loc:xcoordinaat',
    #                               'loc:puntlocatie.loc:puntgeometrie.loc:lambert72.loc:ycoordinaat',
    #                               'loc:puntlocatie.loc:puntgeometrie.loc:lambert72.loc:zcoordinaat',
    #                               'loc:puntlocatie.loc:weglocatie.loc:gemeente', 'loc:puntlocatie.loc:weglocatie.loc:ident2',
    #                               'loc:puntlocatie.loc:weglocatie.loc:ident8',
    #                               'loc:puntlocatie.loc:weglocatie.loc:referentiepaalAfstand',
    #                               'loc:puntlocatie.loc:weglocatie.loc:referentiepaalOpschrift',
    #                               'loc:puntlocatie.loc:weglocatie.loc:straatnaam']
    #
    #         params = {}
    #         for attribuut in locatie_attributen:
    #             if attribuut in flattened_dict.keys():
    #                 params[attribuut] = flattened_dict[attribuut]
    #             else:
    #                 params[attribuut] = None
    #
    #
    #
    #         self.tx_context.run(f"MATCH (a:{ns}:{assettype} "
    #                             "{uuid: $uuid}) SET a += $params",
    #                             uuid=flattened_dict['assetId.identificator'][0:36],
    #                             params=params)
    #     logging.info('done')

    @staticmethod
    def delete_geometrie_records(cursor, uuids):
        if len(uuids) == 0:
            return

        values = "'" + "','".join(uuids) + "'"
        update_query = f"""DELETE FROM geometrie WHERE assetUuid IN ({values})"""
        cursor.execute(update_query)
