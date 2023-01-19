from AssetTypeUpdater import AssetTypeUpdater
from EMInfraImporter import EMInfraImporter
from FastFiller import FastFiller
from PostGISConnector import PostGISConnector


class AssetTypeFiller(FastFiller):
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter, resource: str):
        super().__init__(resource=resource, postgis_connector=postgis_connector, eminfra_importer=eminfra_importer,
                         updater=AssetTypeUpdater(postgis_connector=postgis_connector,
                                                  eminfra_importer=eminfra_importer))

