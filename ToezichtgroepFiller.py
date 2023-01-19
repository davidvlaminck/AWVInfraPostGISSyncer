from EMInfraImporter import EMInfraImporter
from FastFiller import FastFiller
from PostGISConnector import PostGISConnector
from ToezichtgroepUpdater import ToezichtgroepUpdater


class ToezichtgroepFiller(FastFiller):
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter, resource: str):
        super().__init__(resource=resource, postgis_connector=postgis_connector, eminfra_importer=eminfra_importer,
                         updater=ToezichtgroepUpdater())
