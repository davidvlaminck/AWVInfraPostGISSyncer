import json
import logging
import sys

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

environment = 'prd'
settings_manager = SettingsManager(
    settings_path='/home/david/Documents/AWV/resources/settings_AwvinfraPostGISSyncer.json')
db_settings = settings_manager.settings['databases'][environment]

connector = PostGISConnector(**db_settings)
requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env=environment,
                                              multiprocessing_safe=True)
request_handler = RequestHandler(requester)
eminfra_importer = EMInfraImporter(request_handler)

connection = connector.get_connection()
cursor = connection.cursor()
cursor.execute("SELECT uuid, uri, geometrie FROM assettypes LIMIT 100")
rows = cursor.fetchall()
cursor.close()
connector.kill_connection(connection)

uuids = [str(row[0]) for row in rows]

assettypes_with_geometry = set()
for assettype_dict in eminfra_importer.get_assettypes_with_kenmerk_geometrie_by_uuids(uuids):
    assettypes_with_geometry.add(assettype_dict['uuid'])

logging.info(f"Checked {len(uuids)} assettypes via API")
for row in rows:
    uuid, uri, has_geometry_db = row
    has_geometry_api = uuid in assettypes_with_geometry
    mismatch = " <-- MISMATCH" if has_geometry_db != has_geometry_api else ""
    logging.info(f"uuid={uuid} uri={uri} db_geometrie={has_geometry_db} api_has_geometry={has_geometry_api}{mismatch}")