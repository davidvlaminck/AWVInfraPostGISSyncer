import logging
import time

from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Exceptions.BeheerderMissingError import BeheerderMissingError
from Helpers import turn_list_of_lists_into_string


class SchadebeheerderGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer: EMInfraImporter):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started updating schadebeheerder')
        start = time.time()

        asset_dicts = self.eminfra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        amount = self.process_dicts(connection=connection, asset_dicts=list(asset_dicts))

        end = time.time()
        logging.info(f'updated schadebeheerder of {amount} asset(s) in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def process_dicts(connection, asset_dicts: [dict]):
        counter = 0
        values_array = []
        with connection.cursor() as cursor:
            counter += 1
            for asset_dict in asset_dicts:
                uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]

                record_array = [f"'{uuid}'"]

                if 'tz:Schadebeheerder.schadebeheerder' not in asset_dict:
                    record_array.append('NULL')
                else:
                    record_array.append(f"'{asset_dict['tz:Schadebeheerder.schadebeheerder']['tz:DtcBeheerder.referentie']}'")

                values_array.append(record_array)

            values_string = turn_list_of_lists_into_string(values_array)

            if values_string != '':
                check_beheerders_missing_query = f"""
                    WITH s (assetUuid, beheerderReferentie) 
                        AS (VALUES {values_string}),
                    to_update AS (    
                        SELECT s.assetUuid::uuid AS assetUuid, beheerderReferentie, beheerders.uuid as beheerderUuid
                        FROM s
                            LEFT JOIN public.beheerders on s.beheerderReferentie = beheerders.referentie)    
                    SELECT count(*) FROM to_update WHERE beheerderReferentie IS NOT NULL AND beheerderUuid IS NULL;"""

                update_beheerder_query = f"""
                    WITH s (assetUuid, beheerderReferentie) 
                        AS (VALUES {values_string}),
                    to_update AS (
                        SELECT s.assetUuid::uuid AS assetUuid, beheerders.uuid as beheerderUuid
                        FROM s
                            LEFT JOIN public.beheerders on s.beheerderReferentie = beheerders.referentie)        
                    UPDATE assets 
                    SET schadebeheerder = to_update.beheerderUuid
                    FROM to_update 
                    WHERE to_update.assetUuid = assets.uuid"""
                cursor.execute(check_beheerders_missing_query)
                count_beheerders_missing = cursor.fetchone()[0]
                if count_beheerders_missing > 0:
                    logging.error('raising BeheerderMissingError')
                    connection.rollback()
                    raise BeheerderMissingError()

                cursor.execute(update_beheerder_query)

        return counter
