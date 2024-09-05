import logging

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from SyncManager import SyncManager

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    environment = 'aim'

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

    syncer.start()

    # set up database users
    # install postgis: CREATE EXTENSION postgis;

    # too many connections: https://www.cybertec-postgresql.com/en/terminating-database-connections-in-postgresql/