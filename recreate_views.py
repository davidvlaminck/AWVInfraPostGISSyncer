import logging

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from AssetTypeUpdater import AssetTypeUpdater

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

environment = 'prd'
settings_manager = SettingsManager(
    settings_path='/home/david/Documents/AWV/resources/settings_AwvinfraPostGISSyncer.json')
db_settings = settings_manager.settings['databases'][environment]

connector = PostGISConnector(**db_settings)

connection = connector.get_connection()

logging.info('Recreating asset_views with safe casts...')
AssetTypeUpdater.create_views_for_assettypes_with_attributes(connection=connection)
connection.commit()
logging.info('Done! All views in asset_views have been recreated with safe casts.')
logging.info('You can now run main_linux_update_daily_views.py to rebuild asset_daily_views.')

connector.kill_connection(connection)