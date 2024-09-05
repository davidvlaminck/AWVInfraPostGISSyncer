from AssetUpdater import AssetUpdater
from EMInfraImporter import EMInfraImporter
from BaseFiller import BaseFiller
from FillManager import FillManager
from PostGISConnector import PostGISConnector
from ResourceEnum import ResourceEnum


class AssetFiller(BaseFiller):
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter, resource: ResourceEnum,
                 fill_manager: FillManager):
        super().__init__(resource=resource, postgis_connector=postgis_connector, eminfra_importer=eminfra_importer,
                         updater=AssetUpdater(), fill_manager=fill_manager)

