import logging
import time

from EMInfraImporter import EMInfraImporter
from EventProcessors.SpecificEventProcessor import SpecificEventProcessor
from Exceptions.IdentiteitMissingError import IdentiteitMissingError
from Exceptions.ToezichtgroepMissingError import ToezichtgroepMissingError
from PostGISConnector import PostGISConnector


class ToezichtGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, em_infra_importer: EMInfraImporter, connector: PostGISConnector):
        super().__init__(cursor, em_infra_importer)
        self.connector = connector

    def process(self, uuids: [str]):
        logging.info(f'started updating toezicht')
        start = time.time()

        asset_dicts = self.em_infra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        self.process_dicts(cursor=self.cursor, asset_uuids=uuids, asset_dicts=asset_dicts)

        end = time.time()
        logging.info(f'updated {len(asset_dicts)} toezicht in {str(round(end - start, 2))} seconds.')

    def process_dicts(self, cursor, asset_uuids: [str], asset_dicts: [dict]):
        logging.info(f'started changing toezicht of {len(asset_dicts)} assets')

        toezichter_null_assets = []
        toezichter_update_values = ''
        toezichtgroep_null_assets = []
        toezichtgroep_update_values = ''
        toezichter_gebruikersnamen = set()
        toezichtgroepen_referenties = set()

        for asset_dict in asset_dicts:
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]
            if 'tz:Toezicht.toezichtgroep' not in asset_dict:
                toezichtgroep_null_assets.append(uuid)
            else:
                toezichtgroepen_referenties.add(asset_dict['tz:Toezicht.toezichtgroep']['tz:DtcToezichtGroep.referentie'])
                toezichtgroep_update_values += f"('{uuid}', '{asset_dict['tz:Toezicht.toezichtgroep']['tz:DtcToezichtGroep.referentie']}'),"

            if 'tz:Toezicht.toezichter' not in asset_dict:
                toezichter_null_assets.append(uuid)
            else:
                toezichter_gebruikersnamen.add(asset_dict['tz:Toezicht.toezichter']['tz:DtcToezichter.gebruikersnaam'])
                toezichter_update_values += f"('{uuid}', '{asset_dict['tz:Toezicht.toezichter']['tz:DtcToezichter.gebruikersnaam']}'),"

        if len(toezichter_gebruikersnamen) > 0:
            lookup_toezichter_query = f"""SELECT count(*) FROM public.identiteiten WHERE identiteiten.gebruikersnaam IN
                ('{"','".join(list(toezichter_gebruikersnamen))}')"""
            cursor.execute(lookup_toezichter_query)
            toezichter_count = cursor.fetchone()[0]
            if toezichter_count != len(toezichter_gebruikersnamen):
                raise IdentiteitMissingError()

        if len(toezichtgroepen_referenties) > 0:
            lookup_toezichtgroep_query = f"""SELECT count(*) FROM public.toezichtgroepen WHERE toezichtgroepen.referentie IN
                ('{"','".join(list(toezichtgroepen_referenties))}')"""
            cursor.execute(lookup_toezichtgroep_query)
            toezichtgroep_count = cursor.fetchone()[0]
            if toezichtgroep_count != len(toezichtgroepen_referenties):
                raise ToezichtgroepMissingError()

        if len(toezichter_null_assets) > 0:
            delete_toezichter_query = f"""UPDATE public.assets SET toezichter = NULL WHERE uuid IN ('{"'::uuid,'".join(toezichter_null_assets)}'::uuid)"""
            cursor.execute(delete_toezichter_query)
        if len(toezichter_null_assets) > 0:
            delete_toezichtgroep_query = f"""UPDATE public.assets SET toezichtgroep = NULL WHERE uuid IN ('{"'::uuid,'".join(toezichter_null_assets)}'::uuid)"""
            cursor.execute(delete_toezichtgroep_query)

        if toezichter_update_values != '':
            update_toezichter_query = f"""
                WITH s (assetUuid, toezichterGebruikersnaam) 
                    AS (VALUES {toezichter_update_values[:-1]}),
                to_update AS (
                    SELECT s.assetUuid::uuid AS assetUuid, identiteiten.uuid as toezichterUuid
                    FROM s
                        LEFT JOIN public.identiteiten on s.toezichterGebruikersnaam = identiteiten.gebruikersnaam)        
                UPDATE assets 
                SET toezichter = to_update.toezichterUuid
                FROM to_update 
                WHERE to_update.assetUuid = assets.uuid"""
            cursor.execute(update_toezichter_query)

        if toezichtgroep_update_values != '':
            update_toezichtgroep_query = f"""
                WITH s (assetUuid, toezichtgroepReferentie) 
                    AS (VALUES {toezichtgroep_update_values[:-1]}),
                to_update AS (
                    SELECT s.assetUuid::uuid AS assetUuid, toezichtgroepen.uuid as toezichtgroepUuid
                    FROM s
                        LEFT JOIN public.toezichtgroepen on s.toezichtgroepReferentie = toezichtgroepen.referentie)        
                UPDATE assets 
                SET toezichtgroep = to_update.toezichtgroepUuid
                FROM to_update 
                WHERE to_update.assetUuid = assets.uuid"""
            cursor.execute(update_toezichtgroep_query)

        logging.info('done changing toezicht')
