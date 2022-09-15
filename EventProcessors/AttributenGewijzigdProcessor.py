import logging
import time

from EMInfraImporter import EMInfraImporter
from EventProcessors.SpecificEventProcessor import SpecificEventProcessor


class AttributenGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, em_infra_importer: EMInfraImporter):
        super().__init__(cursor, em_infra_importer)

    def process(self, uuids: [str]):
        logging.info(f'started updating attributes')
        start = time.time()

        asset_dicts = self.em_infra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        self.process_dicts(cursor=self.cursor, asset_uuids=uuids, asset_dicts=asset_dicts)

        end = time.time()
        logging.info(f'updated {len(asset_dicts)} assets in {str(round(end - start, 2))} seconds.')

    def process_dicts(self, cursor, asset_uuids, asset_dicts):
        self.remove_existing_attributes(cursor=cursor, asset_uuids=asset_uuids)
        values = self.create_values_string_from_dicts(assets_dicts=asset_dicts, cursor=cursor)
        self.perform_update_with_values(cursor=cursor, values=values)

    @staticmethod
    def remove_existing_attributes(asset_uuids, cursor):
        if len(asset_uuids) == 0:
            return
        delete_query = "DELETE FROM public.attribuutWaarden WHERE assetUuid IN (VALUES ('" + "'::uuid),('".join(
            asset_uuids) + "'::uuid));"

        cursor.execute(delete_query)

    @staticmethod
    def create_values_string_from_dicts(assets_dicts, cursor):
        values = ''
        for asset_dict in assets_dicts:
            asset_uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]
            for key, value in asset_dict.items():
                if key in ['@type', '@id', 'NaampadObject.naampad', 'AIMObject.notitie', 'AIMObject.typeURI', 'AIMDBStatus.isActief', 'AIMNaamObject.naam', 'AIMToestand.toestand']:
                    continue
                if key.startswith('tz:') or key.startswith('geo:') or key.startswith('loc:'):
                    continue
                if key.startswith('lgc:') or key.startswith('ond:') or key.startswith('ins:'):
                    key = key[4:]
                if isinstance(value, dict):
                    value = str(value)
                elif isinstance(value, list):
                    value_list = ''
                    for item in value:
                        value_list += str(item) + '|'
                    if len(value_list) > 0:
                        value = value_list[:-1]
                    else:
                        value = ''
                if not isinstance(value, str):
                    value = str(value)
                value = value.replace("'", "''")
                values += f"('{asset_uuid}','{key}', '{value}'),\n"

        return values

    @staticmethod
    def perform_update_with_values(cursor, values):
        insert_query = f"""
WITH s (assetUuid, attribute_name, waarde) 
    AS (VALUES {values[:-2]}),
to_insert AS (
    SELECT assetUuid::uuid, waarde, attributen.uuid::uuid AS attribuutUuid 
    FROM s 
        LEFT JOIN attributen ON attributen.uri LIKE '%' || '#' || attribute_name)
INSERT INTO public.attribuutWaarden (assetUuid, attribuutUuid, waarde)
SELECT to_insert.assetUuid, to_insert.attribuutUuid, to_insert.waarde
FROM to_insert;"""
        cursor.execute(insert_query)


    def process_dicts2(self, assetDicts):
        asset_processor = NieuwAssetProcessor()
        logging.info(f'started changing eigenschappen of {len(assetDicts)} assets')
        for asset_dict in assetDicts:
            flattened_dict = asset_processor.flatten_dict(input_dict=asset_dict)

            korte_uri = flattened_dict['typeURI'].split('/ns/')[1]
            ns = korte_uri.split('#')[0]
            assettype = korte_uri.split('#')[1]
            if '-' in assettype:
                assettype = '`' + assettype + '`'

            excluded_attributes = ['@type', '@id', 'assetIdUri', 'assetId.identificator', 'assetId.toegekendDoor', 'isActief',
                                   'notitie', 'naam', 'naampad', 'tz:schadebeheerder.tz:naam', 'typeURI',
                                   'tz:schadebeheerder.tz:referentie', 'toestand', 'tz:toezichter.tz:gebruikersnaam',
                                   'tz:toezichter.tz:voornaam', 'tz:toezichter.tz:email',
                                   'tz:toezichter.tz:naam', 'tz:toezichtgroep.tz:naam', 'tz:toezichtgroep.tz:referentie',
                                   'geometry', 'loc:geometrie', 'loc:omschrijving', 'loc:puntlocatie.loc:adres.loc:bus',
                                   'loc:puntlocatie.loc:adres.loc:gemeente', 'loc:puntlocatie.loc:adres.loc:nummer',
                                   'loc:puntlocatie.loc:adres.loc:postcode', 'loc:puntlocatie.loc:adres.loc:provincie',
                                   'loc:puntlocatie.loc:adres.loc:straat', 'loc:puntlocatie.loc:bron',
                                   'loc:puntlocatie.loc:precisie',
                                   'loc:puntlocatie.loc:puntgeometrie.loc:lambert72.loc:xcoordinaat',
                                   'loc:puntlocatie.loc:puntgeometrie.loc:lambert72.loc:ycoordinaat',
                                   'loc:puntlocatie.loc:puntgeometrie.loc:lambert72.loc:zcoordinaat',
                                   'loc:puntlocatie.loc:weglocatie.loc:gemeente', 'loc:puntlocatie.loc:weglocatie.loc:ident2',
                                   'loc:puntlocatie.loc:weglocatie.loc:ident8',
                                   'loc:puntlocatie.loc:weglocatie.loc:referentiepaalAfstand',
                                   'loc:puntlocatie.loc:weglocatie.loc:referentiepaalOpschrift',
                                   'loc:puntlocatie.loc:weglocatie.loc:straatnaam']

            params = {}
            for attribuut in flattened_dict.keys():
                if attribuut not in excluded_attributes:
                    params[attribuut] = flattened_dict[attribuut]

            self.tx_context.run(f"MATCH (a:{ns}:{assettype} "
                                "{uuid: $uuid}) SET a += $params",
                                uuid=flattened_dict['assetId.identificator'][0:36],
                                params=params)
        logging.info('done')

