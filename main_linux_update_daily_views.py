import logging

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from SyncManager import SyncManager, SyncerFactory

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    environment = 'prd'

    settings_manager = SettingsManager(
        settings_path='/home/davidlinux/Documents/AWV/resources/settings_AwvinfraPostGISSyncer.json')
    db_settings = settings_manager.settings['databases'][environment]

    connector = PostGISConnector(**db_settings)

    requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env=environment,
                                                  multiprocessing_safe=True)
    request_handler = RequestHandler(requester)

    eminfra_importer = EMInfraImporter(request_handler)
    syncer = SyncManager(connector=connector, request_handler=request_handler, eminfra_importer=eminfra_importer,
                         settings=settings_manager.settings)

    assets_syncer = SyncerFactory.get_syncer_by_feed_name('assets', eminfra_importer=eminfra_importer,
                                                          postgis_connector=syncer.connector)
    connection = syncer.connector.get_connection()
    assets_syncer.update_view_tables(connection=connection, color=assets_syncer.color)
    # about 35 minutes runtime