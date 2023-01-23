import logging
import time

import psycopg2

from EMInfraImporter import EMInfraImporter
from EventProcessors.SpecificEventProcessor import SpecificEventProcessor
from Exceptions.AssetMissingError import AssetMissingError
from Exceptions.RelatieTypeMissingError import RelatieTypeMissingError
from PostGISConnector import PostGISConnector


class AssetRelatiesGewijzigdProcessor(SpecificEventProcessor):
    def __init__(self, cursor, eminfra_importer: EMInfraImporter, connector: PostGISConnector):
        super().__init__(cursor, eminfra_importer)
        self.connector = connector

    def process(self, uuids: [str]):
        logging.info(f'started updating assetrelaties')
        start = time.time()

        assetrelatie_dicts = self.em_infra_importer.import_assetrelaties_from_webservice_by_assetuuids(asset_uuids=uuids)
        self.remove_all_asset_relaties(cursor=self.cursor, asset_uuids=list(set(uuids)))
        self.process_dicts(cursor=self.cursor, assetrelatie_dicts=assetrelatie_dicts)

        end = time.time()
        logging.info(f'updated {len(assetrelatie_dicts)} assetrelaties in {str(round(end - start, 2))} seconds.')

    def process_dicts(self, cursor, assetrelatie_dicts: dict):
        logging.info(f'started creating {len(assetrelatie_dicts)} assetrelaties')

        values = ''
        for assetrelatie_dict in assetrelatie_dicts:
            if 'RelatieObject.assetId' in assetrelatie_dict:
                uuid = assetrelatie_dict['RelatieObject.assetId']['DtcIdentificator.identificator'][0:36]
            else:
                uuid = assetrelatie_dict['@id'].replace('https://data.awvvlaanderen.be/id/assetrelatie/', '')[0:36]

            if 'RelatieObject.bronAssetId' in assetrelatie_dict:
                bron_uuid = assetrelatie_dict['RelatieObject.bronAssetId']['DtcIdentificator.identificator'][0:36]
            else:
                bron_uuid = assetrelatie_dict['RelatieObject.bron']['@id'].replace(
                    'https://data.awvvlaanderen.be/id/asset/', '')[0:36]

            if bron_uuid == '0beb911a-7bc8-410d-a393-b7e0a1441ef2':
                print('stop')

            if 'RelatieObject.doelAssetId' in assetrelatie_dict:
                doel_uuid = assetrelatie_dict['RelatieObject.doelAssetId']['DtcIdentificator.identificator'][0:36]
            else:
                doel_uuid = assetrelatie_dict['RelatieObject.doel']['@id'].replace(
                    'https://data.awvvlaanderen.be/id/asset/', '')[0:36]

            if 'RelatieObject.typeURI' in assetrelatie_dict:
                relatie_type_uri = assetrelatie_dict['RelatieObject.typeURI']
            else:
                relatie_type_uri = assetrelatie_dict['@type']

            if relatie_type_uri is None:
                print('stop')

            if 'AIMDBStatus.isActief' in assetrelatie_dict:
                actief = assetrelatie_dict['AIMDBStatus.isActief']
            else:
                actief = True

            attributen_dict = assetrelatie_dict.copy()
            for key in ['@type', '@id', "RelatieObject.doel", "RelatieObject.assetId", "AIMDBStatus.isActief",
                         "RelatieObject.bronAssetId", "RelatieObject.doelAssetId", "RelatieObject.typeURI", "RelatieObject.bron"]:
                attributen_dict.pop(key, None)
            attributen = str(attributen_dict).replace("'", "''")
            if attributen == '{}':
                attributen = ''

            values += f"('{uuid}', '{bron_uuid}', '{doel_uuid}', '{relatie_type_uri}', {actief}, "
            if attributen == '':
                values += 'NULL),'
            else:
                values += f"'{attributen}'),"

        insert_query = f"""
WITH s (uuid, bronUuid, doelUuid, relatieTypeUri, actief, attributen) 
    AS (VALUES {values[:-1]}),
to_insert AS (
    SELECT s.uuid::uuid AS uuid, bronUuid::uuid AS bronUuid, doelUuid::uuid AS doelUuid, 
        relatietypes.uuid as relatietype, s.actief, attributen
    FROM s
        LEFT JOIN relatietypes ON relatietypes.uri = s.relatieTypeUri)        
INSERT INTO public.assetrelaties (uuid, bronUuid, doelUuid, relatietype, actief, attributen) 
SELECT to_insert.uuid, to_insert.bronUuid, to_insert.doelUuid, to_insert.relatietype, to_insert.actief, to_insert.attributen
FROM to_insert;"""

        try:
            cursor.execute(insert_query)
        except psycopg2.Error as exc:
            if str(exc).split('\n')[0] == 'insert or update on table "assetrelaties" violates foreign key constraint "assetrelaties_bronuuid_fkey"' or \
                str(exc).split('\n')[0] == 'insert or update on table "assetrelaties" violates foreign key constraint "assetrelaties_doeluuid_fkey"':
                if '\n' in str(exc):
                    print(str(exc).split('\n')[1])
                self.connector.connection.rollback()
                cursor = self.connector.connection.cursor()
                asset_uuids = set(map(lambda x: x['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36], assetrelatie_dicts))
                asset_uuids.update(set(map(lambda x: x['RelatieObject.doel']['@id'].replace('https://data.awvvlaanderen.be/id/asset/', '')[0:36], assetrelatie_dicts)))
                select_assets_query = f"""SELECT uuid FROM public.assets WHERE uuid IN ('{"'::uuid,'".join(asset_uuids)}'::uuid)"""
                cursor.execute(select_assets_query)
                existing_asset_uuids = set(map(lambda x: x[0], cursor.fetchall()))
                nonexisting_assets = list(asset_uuids - existing_asset_uuids)
                raise AssetMissingError(nonexisting_assets)
            elif str(exc).split('\n')[0] == 'null value in column "relatietype" violates not-null constraint':
                raise RelatieTypeMissingError()
            else:
                raise exc
        logging.info('done batch of assetrelaties')

    def remove_all_asset_relaties(self, cursor, asset_uuids):
        if len(asset_uuids) == 0:
            return
        cursor.execute(f"""
        DELETE FROM public.assetrelaties 
        WHERE bronUuid in ('{"','".join(asset_uuids)}') or doelUuid in ('{"','".join(asset_uuids)}')""")
