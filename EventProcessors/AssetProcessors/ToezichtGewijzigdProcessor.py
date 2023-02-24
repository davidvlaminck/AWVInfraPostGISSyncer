import logging
import time

from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from Exceptions.IdentiteitMissingError import IdentiteitMissingError
from Exceptions.ToezichtgroepMissingError import ToezichtgroepMissingError
from Helpers import turn_list_of_lists_into_string


class ToezichtGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, eminfra_importer: EMInfraImporter):
        super().__init__(eminfra_importer)

    def process(self, uuids: [str], connection):
        logging.info(f'started updating toezicht')
        start = time.time()

        asset_dicts = self.eminfra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        amount = self.process_dicts(connection=connection, asset_uuids=uuids, asset_dicts=asset_dicts)

        end = time.time()
        logging.info(f'updated toezicht of {amount} asset(s) in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def process_dicts(connection, asset_uuids: [str], asset_dicts: [dict]):
        counter = 0
        values_array = []
        with connection.cursor() as cursor:
            counter += 1
            for asset_dict in asset_dicts:
                uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]

                record_array = [f"'{uuid}'"]

                if 'tz:Toezicht.toezichtgroep' not in asset_dict:
                    record_array.append('NULL')
                else:
                    record_array.append(
                        f"'{asset_dict['tz:Toezicht.toezichtgroep']['tz:DtcToezichtGroep.referentie']}'")

                if 'tz:Toezicht.toezichter' not in asset_dict:
                    record_array.append('NULL')
                else:
                    record_array.append(
                        f"'{asset_dict['tz:Toezicht.toezichter']['tz:DtcToezichter.gebruikersnaam']}'")

                values_array.append(record_array)

            values_string = turn_list_of_lists_into_string(values_array)

            if values_string != '':
                check_toezicht_missing_query = f"""
        WITH s (assetUuid, toezichtgroepReferentie, toezichterGebruikersnaam) 
            AS (VALUES {values_string}),
        to_update AS (    
            SELECT s.assetUuid::uuid AS assetUuid, toezichtgroepReferentie, toezichtgroepen.uuid as toezichtgroepUuid, 
                toezichterGebruikersnaam, identiteiten.uuid as toezichterUuid
            FROM s
                LEFT JOIN public.toezichtgroepen on s.toezichtgroepReferentie = toezichtgroepen.referentie
                LEFT JOIN public.identiteiten on s.toezichterGebruikersnaam = identiteiten.gebruikersnaam)    
        SELECT SUM(CASE WHEN toezichtgroepReferentie IS NOT NULL AND toezichtgroepUuid IS NULL THEN 1 else 0 END) 
            as toezichtgroep_missing, 
            SUM(CASE WHEN toezichterGebruikersnaam IS NOT NULL AND toezichterUuid IS NULL THEN 1 else 0 END) 
            as toezichter_missing
        FROM to_update;"""
                cursor.execute(check_toezicht_missing_query)
                count_toezichtgroep_missing, count_toezichter_missing = cursor.fetchone()
                if count_toezichtgroep_missing > 0:
                    logging.error('raising ToezichtgroepMissingError')
                    connection.rollback()
                    raise ToezichtgroepMissingError()
                if count_toezichter_missing > 0:
                    logging.error('raising IdentiteitMissingError')
                    connection.rollback()
                    raise IdentiteitMissingError()

                update_toezicht_query = f"""
            WITH s (assetUuid, toezichtgroepReferentie, toezichterGebruikersnaam) 
                AS (VALUES {values_string}),
            to_update AS (
                SELECT s.assetUuid::uuid AS assetUuid, toezichtgroepen.uuid as toezichtgroepUuid, 
                    identiteiten.uuid as toezichterUuid
                FROM s
                    LEFT JOIN public.identiteiten on s.toezichterGebruikersnaam = identiteiten.gebruikersnaam
                    LEFT JOIN public.toezichtgroepen on s.toezichtgroepReferentie = toezichtgroepen.referentie)                
            UPDATE assets 
            SET toezichtgroep = to_update.toezichtgroepUuid, toezichter = to_update.toezichterUuid
            FROM to_update 
            WHERE to_update.assetUuid = assets.uuid"""
                cursor.execute(update_toezicht_query)

        return counter
