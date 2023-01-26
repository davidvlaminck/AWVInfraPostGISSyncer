import logging
import time
from typing import Iterator

import psycopg2

from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.AttributenGewijzigdProcessor import AttributenGewijzigdProcessor
from EventProcessors.AssetProcessors.ElekAansluitingGewijzigdProcessor import ElekAansluitingGewijzigdProcessor
from EventProcessors.AssetProcessors.GeometrieOrLocatieGewijzigdProcessor import GeometrieOrLocatieGewijzigdProcessor
from EventProcessors.AssetProcessors.NieuwAssetProcessor import NieuwAssetProcessor
from EventProcessors.AssetProcessors.SchadebeheerderGewijzigdProcessor import SchadebeheerderGewijzigdProcessor
from EventProcessors.AssetProcessors.ToezichtGewijzigdProcessor import ToezichtGewijzigdProcessor
from Helpers import peek_generator
from PostGISConnector import PostGISConnector


class AssetUpdater:
    @staticmethod
    def update_objects(object_generator: Iterator[dict], connection, insert_only: bool = False) -> int:
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return 0

        values = ''
        counter = 0
        asset_dict_list = []
        asset_uuids = []
        for asset_dict in object_generator:
            counter += 1
            asset_dict_list.append(asset_dict)
            asset_uuid = asset_dict['@id'].split('/')[-1][0:36]
            asset_uuids.append(asset_uuid)

            values = AssetUpdater.append_values(asset_dict, asset_uuid, values)

        AssetUpdater.perform_insert_update_from_values(connection, insert_only, values)

        AttributenGewijzigdProcessor.process_dicts(connection=connection, asset_uuids=asset_uuids,
                                                   asset_dicts=asset_dict_list)
        SchadebeheerderGewijzigdProcessor.process_dicts(connection=connection, asset_uuids=asset_uuids,
                                                        asset_dicts=asset_dict_list)

        toezicht_processor = ToezichtGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer)
        toezicht_processor.process_dicts(cursor=cursor, asset_uuids=uuids, asset_dicts=asset_dicts)

        self.update_location_geometry_of_synced_assets(uuids, asset_dicts, cursor)

        self.update_elek_aansluiting_of_synced_assets(asset_dicts, cursor, uuids)

        return counter

    @staticmethod
    def perform_insert_update_from_values(connection, insert_only, values):
        insert_query = f"""
WITH s (uuid, assetTypeUri, actief, toestand, naampad, naam, commentaar) 
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, assettypes.uuid as assettype, actief, toestand, naampad, naam, commentaar
    FROM s
        LEFT JOIN assettypes ON assettypes.uri = s.assetTypeUri),
to_insert AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.assets AS assets ON assets.uuid = t.uuid 
    WHERE assets.uuid IS NULL)
INSERT INTO public.assets (uuid, assettype, actief, toestand, naampad, naam, commentaar) 
SELECT to_insert.uuid, to_insert.assettype, to_insert.actief, to_insert.toestand, to_insert.naampad, to_insert.naam, 
    to_insert.commentaar
FROM to_insert;"""
        update_query = ''
        if not insert_only:
            update_query = f"""
WITH s (uuid, assetTypeUri, actief, toestand, naampad, naam, commentaar)  
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, assettypes.uuid as assettype, actief, toestand, naampad, naam, commentaar
    FROM s
        LEFT JOIN assettypes ON assettypes.uri = s.assetTypeUri),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.assets AS assets ON assets.uuid = t.uuid 
    WHERE assets.uuid IS NOT NULL)
UPDATE assets 
SET actief = to_update.actief, toestand = to_update.toestand, naampad = to_update.naampad, naam = to_update.naam, 
    commentaar = to_update.commentaar
FROM to_update 
WHERE to_update.uuid = assets.uuid;"""
        with connection.cursor() as cursor:
            cursor.execute(insert_query)
        if not insert_only:
            with connection.cursor() as cursor:
                cursor.execute(update_query)

    @staticmethod
    def append_values(asset_dict, asset_uuid, values):
        if 'AIMDBStatus.isActief' in asset_dict:
            actief = asset_dict['AIMDBStatus.isActief']
        else:
            actief = True
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
        elif 'AbstracteAanvullendeGeometrie.naam' in asset_dict:
            naam = asset_dict['AbstracteAanvullendeGeometrie.naam'].replace("'", "''")
        commentaar = None
        if 'AIMObject.notitie' in asset_dict:
            commentaar = asset_dict['AIMObject.notitie'].replace("'", "''").replace("\n", " ")
        values += f"('{asset_uuid}','{asset_dict['@type']}',{actief},"
        for attribute in [toestand, naampad, naam, commentaar]:
            if attribute is None or attribute == '':
                values += 'NULL,'
            else:
                values += f"'{attribute}',"
        values = values[:-1] + '),'
        return values


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

            attributen_processor = AttributenGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer)
            attributen_processor.process_dicts(cursor=cursor, asset_uuids=uuids, asset_dicts=asset_dicts)
            schadebeheerder_processor = SchadebeheerderGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer)
            schadebeheerder_processor.process_dicts(cursor=cursor, asset_uuids=uuids, asset_dicts=asset_dicts)
            toezicht_processor = ToezichtGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer)
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
                                                                       eminfra_importer=self.eminfra_importer)
        elek_aansluiting_processor.process(uuids=assets_for_elek_aansluiting)
        end = time.time()
        logging.info(f'updated elek aansluiting of {len(assets_for_elek_aansluiting)} assets in {str(round(end - start, 2))} seconds.')

    def update_location_geometry_of_synced_assets(self, uuids, asset_dicts, cursor):
        start = time.time()
        geometry_processor = GeometrieOrLocatieGewijzigdProcessor(cursor=cursor,
                                                                  eminfra_importer=self.eminfra_importer)
        geometry_processor.process_dicts(uuids=uuids, asset_dicts=asset_dicts)
        end = time.time()
        logging.info(f'updated location/geometry of {len(asset_dicts)} assets in {str(round(end - start, 2))} seconds.')
        return uuids

    def update_assets(self, cursor: psycopg2._psycopg.cursor, assets_dicts: [dict]):
        if len(assets_dicts) == 0:
            return

        values = NieuwAssetProcessor.create_values_string_from_dicts(cursor=cursor, assets_dicts=assets_dicts)
        NieuwAssetProcessor.perform_insert_with_values(cursor=cursor, values=values)
        self.perform_update_with_values(cursor=cursor, values=values)

        self.postGIS_connector.connection.commit()

        # TODO parent aanpassen > sql query op databank zelf uitvoeren
        # beheerobjecten nodig

    @staticmethod
    def perform_update_with_values(cursor: psycopg2._psycopg.cursor, values):
        update_query = f"""
WITH s (uuid, assettype, actief, toestand, naampad, naam, commentaar)  
    AS (VALUES {values[:-1]}),
t AS (
    SELECT uuid::uuid AS uuid, assettype::uuid as assettype, actief, toestand, naampad, naam, commentaar
    FROM s),
to_update AS (
    SELECT t.* 
    FROM t
        LEFT JOIN public.assets AS assets ON assets.uuid = t.uuid 
    WHERE assets.uuid IS NOT NULL)
UPDATE assets 
SET actief = to_update.actief, toestand = to_update.toestand, naampad = to_update.naampad, naam = to_update.naam, 
    commentaar = to_update.commentaar
FROM to_update 
WHERE to_update.uuid = assets.uuid;"""
        cursor.execute(update_query)

