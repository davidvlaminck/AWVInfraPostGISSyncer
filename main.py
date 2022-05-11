import logging

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from Syncer import Syncer

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    connector = PostGISConnector(host="127.0.0.1", user="postgres", password="admin", port="5432",
                                      database="awvinfra")
    settings_manager = SettingsManager(settings_path='C:\\resources\\settings_AwvinfraPostGISSyncer.json')

    requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env='prd')
    request_handler = RequestHandler(requester)

    eminfra_importer = EMInfraImporter(request_handler)
    syncer = Syncer(connector=connector, request_handler=request_handler, eminfra_importer=eminfra_importer, settings=settings_manager.settings)

    syncer.start_syncing()
