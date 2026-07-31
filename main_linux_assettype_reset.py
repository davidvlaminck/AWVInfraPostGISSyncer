import logging

from AssetTypeUpdater import AssetTypeUpdater
from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from RequesterFactory import RequesterFactory
from RequestHandler import RequestHandler
from SettingsManager import SettingsManager

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    environment = 'prd'
    page_size = 100

    settings_manager = SettingsManager(
        settings_path='/home/davidlinux/Documenten/AWV/resources/settings_AwvinfraPostGISSyncer.json')
    db_settings = settings_manager.settings['databases'][environment]

    connector = PostGISConnector(**db_settings)
    connector.set_up_tables()

    requester = RequesterFactory.create_requester(
        settings=settings_manager.settings, auth_type='JWT', env=environment,
        multiprocessing_safe=True)
    request_handler = RequestHandler(requester)

    eminfra_importer = EMInfraImporter(request_handler)
    assettype_updater = AssetTypeUpdater(
        postgis_connector=connector, eminfra_importer=eminfra_importer)

    connection = connector.get_connection()

    try:
        assettypes = eminfra_importer.import_assettypes_from_webservice_page_by_page(
            page_size=page_size)
        assettype_updater.update_objects(
            object_generator=assettypes, connection=connection)
        logger.info('Assettypes updated and views created successfully.')
    except Exception as exc:
        connection.rollback()
        logger.error('Failed to update assettypes: %s', exc)
        raise
    finally:
        connector.kill_connection(connection)
