import logging
import time

from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Exceptions.BeheerderMissingError import BeheerderMissingError


class SchadebeheerderGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, eminfra_importer: EMInfraImporter):
        super().__init__(cursor, eminfra_importer)

    def process(self, uuids: [str]):
        logging.info(f'started updating schadebeheerder')
        start = time.time()

        asset_dicts = self.em_infra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        self.process_dicts(cursor=self.cursor, asset_uuids=uuids, asset_dicts=asset_dicts)

        end = time.time()
        logging.info(f'updated {len(asset_dicts)} schadebeheerder in {str(round(end - start, 2))} seconds.')

    def process_dicts(self, cursor, asset_uuids: [str], asset_dicts: [dict]):
        logging.info(f'started changing schadebeheerder of {len(asset_dicts)} assets')

        beheerder_null_assets = []
        beheerder_update_values = ''
        beheerders_referenties = set()

        for asset_dict in asset_dicts:
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]
            if 'tz:Schadebeheerder.schadebeheerder' not in asset_dict:
                beheerder_null_assets.append(uuid)
            else:
                beheerders_referenties.add(
                    asset_dict['tz:Schadebeheerder.schadebeheerder']['tz:DtcBeheerder.referentie'])
                beheerder_update_values += f"('{uuid}', '{asset_dict['tz:Schadebeheerder.schadebeheerder']['tz:DtcBeheerder.referentie']}'),"

        if len(beheerders_referenties) > 0:
            lookup_beheerder_query = f"""SELECT count(*) FROM public.beheerders WHERE beheerders.referentie IN
                ('{"','".join(list(beheerders_referenties))}')"""
            cursor.execute(lookup_beheerder_query)
            beheerder_count = cursor.fetchone()[0]
            if beheerder_count != len(beheerders_referenties):
                raise BeheerderMissingError()

        if len(beheerder_null_assets) > 0:
            delete_beheerder_query = f"""UPDATE public.assets SET schadebeheerder = NULL WHERE uuid IN ('{"'::uuid,'".join(beheerder_null_assets)}'::uuid)"""
            cursor.execute(delete_beheerder_query)

        if beheerder_update_values != '':
            update_beheerder_query = f"""
                WITH s (assetUuid, beheerderReferentie) 
                    AS (VALUES {beheerder_update_values[:-1]}),
                to_update AS (
                    SELECT s.assetUuid::uuid AS assetUuid, beheerders.uuid as beheerderUuid
                    FROM s
                        LEFT JOIN public.beheerders on s.beheerderReferentie = beheerders.referentie)        
                UPDATE assets 
                SET schadebeheerder = to_update.beheerderUuid
                FROM to_update 
                WHERE to_update.assetUuid = assets.uuid"""
            cursor.execute(update_beheerder_query)

        logging.info('done changing schadebeheerder')
