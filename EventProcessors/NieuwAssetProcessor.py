import logging
import time

import psycopg2

from EventProcessors.SpecificEventProcessor import SpecificEventProcessor
from Exceptions.AssetTypeMissingError import AssetTypeMissingError


class NieuwAssetProcessor(SpecificEventProcessor):
    def __init__(self, cursor, em_infra_importer):
        super().__init__(cursor, em_infra_importer)

    def process(self, uuids: [str]):
        logging.info(f'started creating assets')
        start = time.time()

        asset_dicts = self.em_infra_importer.import_assets_from_webservice_by_uuids(asset_uuids=uuids)
        values = self.create_values_string_from_dicts(cursor=self.cursor, assets_dicts=asset_dicts)
        self.perform_insert_with_values(cursor=self.cursor, values=values)

        end = time.time()
        logging.info(f'created {len(asset_dicts)} assets in {str(round(end - start, 2))} seconds.')

    @staticmethod
    def perform_insert_with_values(cursor: psycopg2._psycopg.cursor, values):
        insert_query = f"""
    WITH s (uuid, assettype, actief, toestand, naampad, naam, commentaar) 
        AS (VALUES {values[:-1]}),
    t AS (
        SELECT uuid::uuid AS uuid, assettype::uuid as assettype, actief, toestand, naampad, naam, commentaar
        FROM s),
    to_insert AS (
        SELECT t.* 
        FROM t
            LEFT JOIN public.assets AS assets ON assets.uuid = t.uuid 
        WHERE assets.uuid IS NULL)
    INSERT INTO public.assets (uuid, assettype, actief, toestand, naampad, naam, commentaar) 
    SELECT to_insert.uuid, to_insert.assettype, to_insert.actief, to_insert.toestand, to_insert.naampad, to_insert.naam, 
        to_insert.commentaar
    FROM to_insert;"""
        cursor.execute(insert_query)

    @staticmethod
    def create_values_string_from_dicts(cursor: psycopg2.extensions.cursor, assets_dicts):
        assettype_uris = list(map(lambda x: x['@type'], assets_dicts))
        assettype_mapping = NieuwAssetProcessor.create_assettype_mapping(cursor=cursor, assettype_uris=assettype_uris)
        values = ''
        for asset_dict in assets_dicts:
            uuid = asset_dict['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36]
            try:
                assettype = assettype_mapping[asset_dict['@type']]
            except KeyError:
                raise AssetTypeMissingError(f"Assettype {asset_dict['@type']} does not exist")

            actief = None
            if 'AIMDBStatus.isActief' in asset_dict:
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
            elif 'AbstracteAanvullendeGeometrie.naam' in asset_dict:
                naam = asset_dict['AbstracteAanvullendeGeometrie.naam'].replace("'", "''")

            commentaar = None
            if 'AIMObject.notitie' in asset_dict:
                commentaar = asset_dict['AIMObject.notitie'].replace("'", "''").replace("\n", " ")

            values += f"('{uuid}','{assettype}',{actief},"
            for attribute in [toestand, naampad, naam, commentaar]:
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
