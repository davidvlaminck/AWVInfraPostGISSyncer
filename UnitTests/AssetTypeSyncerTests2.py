from typing import Dict, Generator
from unittest.mock import Mock

import pytest

from AssetSyncer import AssetSyncer
from AssetTypeUpdater import AssetTypeUpdater
from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.WeglocatieGewijzigdProcessor import WeglocatieGewijzigdProcessor
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager

#
# @pytest.fixture(scope='module')
# def setup():
#     settings_manager = SettingsManager(
#         settings_path='/home/davidlinux/Documents/AWV/resources/settings_AwvinfraPostGISSyncer.json')
#     unittest_db_settings = settings_manager.settings['databases']['unittest']
#     unittest_db_settings['database'] = 'unittests'
#
#     connector = PostGISConnector(**unittest_db_settings)
#
#     cursor = connector.main_connection.cursor()
#
#     truncate_queries = """SELECT 'TRUNCATE ' || input_table_name || ' CASCADE;' AS truncate_query FROM(
#     SELECT table_schema || '.' || table_name AS input_table_name FROM information_schema.tables
#     WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'views') AND table_schema NOT LIKE 'pg_toast%'
#         AND table_name != 'spatial_ref_sys' AND table_type = 'BASE TABLE')
#     AS information; """
#
#     cursor.execute(truncate_queries)
#     truncate_queries = cursor.fetchall()
#
#     for truncate_query in truncate_queries:
#         cursor.execute(truncate_query[0])
#
#     connector.set_up_tables('../setup_tables_querys.sql')
#
#     requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
#     request_handler = RequestHandler(requester)
#     eminfra_importer = EMInfraImporter(request_handler)
#
#     return {'connector': connector, 'eminfra_importer': eminfra_importer}


def test_create_view_easy_test_case(subtests):
    assettype_updater = AssetTypeUpdater(postgis_connector=Mock(), eminfra_importer=Mock())
    AssetTypeUpdater.create_view = Mock()

    def return_mock_data_get_assettypes_with_geometries(connection):
        return [('dcf32dcf-6ea0-4ae6-be2c-7e12273f0f7a',
                 'https://bz.data.wegenenverkeer.be/ns/beheeractie#ConditiemeetingHulppostIdentificatie', False)]
    AssetTypeUpdater.get_assettypes_with_geometries = return_mock_data_get_assettypes_with_geometries

    def return_mock_data_get_attributes_by_type_uri(connection, type_uri):
        return [('107b920c-8c14-4a72-85b2-596c1ffdced5',
                 'ConditiemeetingHulppostIdentificatie.stevigBevestigd', 'boolean'),
                ('867c9367-5248-4dc0-9027-f9217325a5fa',
                 'ConditiemeetingHulppostIdentificatie.goedLeesbaar', 'boolean')]
    AssetTypeUpdater.get_attributes_by_type_uri = return_mock_data_get_attributes_by_type_uri

    assettype_updater.create_views_for_assettypes_with_attributes(connection=Mock())

    _, _, kwargs = assettype_updater.create_view.mock_calls[0]

    expected_query = """DROP VIEW IF EXISTS asset_views.beheeractie_ConditiemeetingHulppostIdentificatie CASCADE;
CREATE VIEW asset_views.beheeractie_ConditiemeetingHulppostIdentificatie AS
    SELECT assets.uuid as uuid, assets.toestand as toestand_asset, assets.actief as actief_asset, 
        assets.naam as naam_asset , attribuut_001.waarde::boolean AS ConditiemeetingHulppostIdentificatie_stevigBevestigd,attribuut_002.waarde::boolean AS ConditiemeetingHulppostIdentificatie_goedLeesbaar
    FROM assets
    LEFT JOIN attribuutwaarden attribuut_001 ON assets.uuid = attribuut_001.assetuuid AND attribuut_001.attribuutuuid = '107b920c-8c14-4a72-85b2-596c1ffdced5'
LEFT JOIN attribuutwaarden attribuut_002 ON assets.uuid = attribuut_002.assetuuid AND attribuut_002.attribuutuuid = '867c9367-5248-4dc0-9027-f9217325a5fa'
 WHERE assettype = 'dcf32dcf-6ea0-4ae6-be2c-7e12273f0f7a' and assets.actief = TRUE;"""

    assert kwargs['create_view_query'] == expected_query



def test_create_view_attributes_too_long(subtests):
    assettype_updater = AssetTypeUpdater(postgis_connector=Mock(), eminfra_importer=Mock())
    AssetTypeUpdater.create_view = Mock()

    def return_mock_data_get_assettypes_with_geometries(connection):
        return [('dcf32dcf-6ea0-4ae6-be2c-7e12273f0f7a',
                 'https://bz.data.wegenenverkeer.be/ns/beheeractie#ConditiemeetingHulppostIdentificatie', False)]
    AssetTypeUpdater.get_assettypes_with_geometries = return_mock_data_get_assettypes_with_geometries

    def return_mock_data_get_attributes_by_type_uri(connection, type_uri):
        return [('107b920c-8c14-4a72-85b2-596c1ffdced5',
                 'ConditiemeetingHulppostIdentificatie.extremely_long_attribute_name', 'boolean'),
                ('867c9367-5248-4dc0-9027-f9217325a5fa',
                 'ConditiemeetingHulppostIdentificatie.extremely_long_attribute_name_2', 'boolean')]
    AssetTypeUpdater.get_attributes_by_type_uri = return_mock_data_get_attributes_by_type_uri

    assettype_updater.create_views_for_assettypes_with_attributes(connection=Mock())

    name, args, kwargs = assettype_updater.create_view.mock_calls[0]

    expected_query = """DROP VIEW IF EXISTS asset_views.beheeractie_ConditiemeetingHulppostIdentificatie CASCADE;
CREATE VIEW asset_views.beheeractie_ConditiemeetingHulppostIdentificatie AS
    SELECT assets.uuid as uuid, assets.toestand as toestand_asset, assets.actief as actief_asset, 
        assets.naam as naam_asset , attribuut_001.waarde::boolean AS ConditiemeetingHulppostIdentificatie_extremely_long_attribut001,attribuut_002.waarde::boolean AS ConditiemeetingHulppostIdentificatie_extremely_long_attribut002
    FROM assets
    LEFT JOIN attribuutwaarden attribuut_001 ON assets.uuid = attribuut_001.assetuuid AND attribuut_001.attribuutuuid = '107b920c-8c14-4a72-85b2-596c1ffdced5'
LEFT JOIN attribuutwaarden attribuut_002 ON assets.uuid = attribuut_002.assetuuid AND attribuut_002.attribuutuuid = '867c9367-5248-4dc0-9027-f9217325a5fa'
 WHERE assettype = 'dcf32dcf-6ea0-4ae6-be2c-7e12273f0f7a' and assets.actief = TRUE;"""

    assert kwargs['create_view_query'] == expected_query