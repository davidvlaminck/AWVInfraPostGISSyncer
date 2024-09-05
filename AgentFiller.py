from AgentUpdater import AgentUpdater
from EMInfraImporter import EMInfraImporter
from BaseFiller import BaseFiller
from PostGISConnector import PostGISConnector
from ResourceEnum import ResourceEnum


class AgentFiller(BaseFiller):
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter, resource: ResourceEnum,
                 fill_manager):
        super().__init__(resource=resource, postgis_connector=postgis_connector, eminfra_importer=eminfra_importer,
                         updater=AgentUpdater(), fill_manager=fill_manager)

