from EMInfraImporter import EMInfraImporter
from BetrokkeneRelatiesUpdater import BetrokkeneRelatiesUpdater
from FastFiller import FastFiller
from PostGISConnector import PostGISConnector


class BetrokkeneRelatieFiller(FastFiller):
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter, resource: str):
        super().__init__(resource=resource, postgis_connector=postgis_connector, eminfra_importer=eminfra_importer,
                         updater=BetrokkeneRelatiesUpdater())
