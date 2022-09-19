import logging
import time

import psycopg2

from EMInfraImporter import EMInfraImporter
from EventProcessors.AttributenGewijzigdProcessor import AttributenGewijzigdProcessor
from EventProcessors.ElekAansluitingGewijzigdProcessor import ElekAansluitingGewijzigdProcessor
from EventProcessors.GeometrieOrLocatieGewijzigdProcessor import GeometrieOrLocatieGewijzigdProcessor
from EventProcessors.SchadebeheerderGewijzigdProcessor import SchadebeheerderGewijzigdProcessor
from EventProcessors.ToezichtGewijzigdProcessor import ToezichtGewijzigdProcessor
from Exceptions.AssetTypeMissingError import AssetTypeMissingError
from PostGISConnector import PostGISConnector


class AssetSyncer:
    def __init__(self, postgis_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postGIS_connector = postgis_connector
        self.eminfra_importer = em_infra_importer

    def sync_assets(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            start = time.time()

            asset_dicts = list(self.eminfra_importer.import_assets_from_webservice_page_by_page(page_size=page_size))
            cursor = self.postGIS_connector.connection.cursor()
            logging.info(f'creating/updating {len(asset_dicts)} assets')
            current_pagingcursor = self.eminfra_importer.pagingcursor

            self.update_assets(cursor=cursor, assets_dicts=asset_dicts)

            uuids = list(map(lambda d: d['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36], asset_dicts))

            attributen_processor = AttributenGewijzigdProcessor(cursor=cursor, em_infra_importer=self.eminfra_importer)
            attributen_processor.process_dicts(cursor=cursor, asset_uuids=uuids, asset_dicts=asset_dicts)
            schadebeheerder_processor = SchadebeheerderGewijzigdProcessor(cursor=cursor, em_infra_importer=self.eminfra_importer)
            schadebeheerder_processor.process_dicts(cursor=cursor, asset_uuids=uuids, asset_dicts=asset_dicts)
            toezicht_processor = ToezichtGewijzigdProcessor(cursor=cursor, em_infra_importer=self.eminfra_importer)
            toezicht_processor.process_dicts(cursor=cursor, asset_uuids=uuids, asset_dicts=asset_dicts)

            self.update_location_geometry_of_synced_assets(uuids, asset_dicts, cursor)

            self.update_elek_aansluiting_of_synced_assets(asset_dicts, cursor, uuids)

            self.postGIS_connector.save_props_to_params({'pagingcursor': current_pagingcursor})

            end = time.time()
            logging.info(f'total time for {len(asset_dicts)} assets: {round(end - start, 2)}')

            if current_pagingcursor == '':
                break

    def update_elek_aansluiting_of_synced_assets(self, asset_dicts, cursor, uuids):
        start = time.time()
        joined_uuids = "','".join(uuids)
        select_assets_for_elek_aansluiting_query = f"""SELECT assets.uuid 
            FROM assets 
                LEFT JOIN assettypes ON assets.assettype = assettypes.uuid
            WHERE assets.uuid IN ('{joined_uuids}')
            AND elek_aansluiting = TRUE;"""
        cursor.execute(select_assets_for_elek_aansluiting_query)
        assets_for_elek_aansluiting = list(map(lambda x: x[0], cursor.fetchall()))
        elek_aansluiting_processor = ElekAansluitingGewijzigdProcessor(cursor=cursor,
                                                                       em_infra_importer=self.eminfra_importer)
        elek_aansluiting_processor.process(uuids=assets_for_elek_aansluiting)
        end = time.time()
        logging.info(f'updated elek aansluiting of {len(assets_for_elek_aansluiting)} assets in {str(round(end - start, 2))} seconds.')

    def update_location_geometry_of_synced_assets(self, uuids, asset_dicts, cursor):
        start = time.time()
        geometry_processor = GeometrieOrLocatieGewijzigdProcessor(cursor=cursor,
                                                                  em_infra_importer=self.eminfra_importer)
        geometry_processor.process_dicts(uuids=uuids, asset_dicts=asset_dicts)
        end = time.time()
        logging.info(f'updated location/geometry of {len(asset_dicts)} assets in {str(round(end - start, 2))} seconds.')
        return uuids

    def update_assets(self, cursor: psycopg2._psycopg.cursor, assets_dicts: [dict]):
        if len(assets_dicts) == 0:
            return

        values = self.create_values_string_from_dicts(cursor=cursor, assets_dicts=assets_dicts)
        self.perform_insert_with_values(cursor=cursor, values=values)
        self.perform_update_with_values(cursor=cursor, values=values)

        self.postGIS_connector.connection.commit()

        # TODO parent aanpassen > sql query op databank zelf uitvoeren
        # beheerobjecten nodig

    @staticmethod
    def perform_update_with_values(cursor: psycopg2._psycopg.cursor, values):
        update_query = f"""
WITH s (uuid, assettype, actief, toestand, naampad, naam, schadebeheerder, toezichter, toezichtgroep, commentaar)  
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, assettype::uuid as assettype, actief, toestand, naampad, naam, 
        schadebeheerder::uuid as schadebeheerder, toezichter::uuid as toezichter, toezichtgroep::uuid as toezichtgroep, commentaar
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.assets AS assets ON assets.uuid = t.uuid 
    WHERE assets.uuid IS NOT NULL)
UPDATE assets 
SET actief = to_update.actief, toestand = to_update.toestand, naampad = to_update.naampad, naam = to_update.naam, 
schadebeheerder = to_update.schadebeheerder, toezichter = to_update.toezichter, toezichtgroep = to_update.toezichtgroep, commentaar = to_update.commentaar
FROM to_update 
WHERE to_update.uuid = assets.uuid;"""
        cursor.execute(update_query)

    @staticmethod
    def perform_insert_with_values(cursor: psycopg2._psycopg.cursor, values):
        insert_query = f"""
WITH s (uuid, assettype, actief, toestand, naampad, naam, schadebeheerder, toezichter, toezichtgroep, commentaar) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, assettype::uuid as assettype, actief, toestand, naampad, naam, 
        schadebeheerder::uuid as schadebeheerder, toezichter::uuid as toezichter, toezichtgroep::uuid as toezichtgroep, commentaar
    FROM s),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.assets AS assets ON assets.uuid = t.uuid 
    WHERE assets.uuid IS NULL)
INSERT INTO public.assets (uuid, assettype, actief, toestand, naampad, naam, schadebeheerder, toezichter, toezichtgroep, commentaar) 
SELECT to_insert.uuid, to_insert.assettype, to_insert.actief, to_insert.toestand, to_insert.naampad, to_insert.naam, 
to_insert.schadebeheerder, to_insert.toezichter, to_insert.toezichtgroep, to_insert.commentaar
FROM to_insert;"""
        cursor.execute(insert_query)

    @staticmethod
    def create_values_string_from_dicts(cursor: psycopg2.extensions.cursor, assets_dicts, full_sync: bool = True):
        assettype_uris = list(map(lambda x: x['@type'], assets_dicts))
        assettype_mapping = AssetSyncer.create_assettype_mapping(cursor=cursor, assettype_uris=assettype_uris)
        values = ''
        for asset_dict in assets_dicts:
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]
            try:
                assettype = assettype_mapping[asset_dict['@type']]
            except KeyError:
                raise AssetTypeMissingError(f"Assettype {asset_dict['@type']} does not exist")

            actief = asset_dict['AIMDBStatus.isActief']

            toestand = None
            if 'AIMToestand.toestand' in asset_dict:
                toestand = asset_dict['AIMToestand.toestand'].replace(
                    'https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/', '')

            naampad = None
            if 'NaampadObject.naampad' in asset_dict:
                naampad = asset_dict['NaampadObject.naampad'].replace("'", "''")

            naam = None
            if 'AIMNaamObject.naam' in asset_dict:
                naam = asset_dict['AIMNaamObject.naam'].replace("'", "''")

            schadebeheerder = None
            toezichter = None
            toezichtgroep = None
            if full_sync:
                if 'tz:Schadebeheerder.schadebeheerder' in asset_dict:
                    schadebeheerder = asset_dict['tz:Schadebeheerder.schadebeheerder']['tz:DtcBeheerder.referentie']
                schadebeheerder = '00000000-0000-0000-0000-000000000000'
                schadebeheerder = None
                # TODO implement schadebeheerder mapping

                if 'tz:Toezicht.toezichter' in asset_dict:
                    toezichter = asset_dict['tz:Toezicht.toezichter']['tz:DtcToezichter.gebruikersnaam']
                toezichter = '00000000-0000-0000-0000-000000000000'
                toezichter = None
                # TODO implement toezichter mapping

                if 'tz:Toezicht.toezichtgroep' in asset_dict:
                    toezichtgroep = asset_dict['tz:Toezicht.toezichtgroep']['tz:DtcToezichtGroep.referentie']
                toezichtgroep = '00000000-0000-0000-0000-000000000000'
                toezichtgroep = None
                # TODO implement toezichtgroep mapping

            commentaar = None
            if 'AIMObject.notitie' in asset_dict:
                commentaar = asset_dict['AIMObject.notitie'].replace("'", "''").replace("\n", " ")

            values += f"('{uuid}','{assettype}',{actief},"
            for attribute in [toestand, naampad, naam, schadebeheerder, toezichter, toezichtgroep, commentaar]:
                if attribute is None or attribute == '':
                    values += 'NULL,'
                else:
                    values += f"'{attribute}',"
            values = values[:-1] + '),'
        return values

    @staticmethod
    def create_assettype_mapping(cursor: psycopg2.extensions.cursor, assettype_uris: [str]) -> dict:
        unique_uris = set(assettype_uris)
        joined_unique_uris = "','".join(unique_uris)

        mapping_table_query = f"SELECT uri, uuid FROM assettypes WHERE uri in ('{joined_unique_uris}')"
        cursor.execute(mapping_table_query)
        results = cursor.fetchall()
        mapping_dict = {}
        for result in results:
            mapping_dict[result[0]] = result[1]

        return mapping_dict
