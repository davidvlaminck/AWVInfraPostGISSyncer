from BaseFiller import BaseFiller
from ControleficheUpdater import ControleficheUpdater
from EMInfraImporter import EMInfraImporter
from FillManager import FillManager
from PostGISConnector import PostGISConnector
from ResourceEnum import ResourceEnum


class ControleficheFiller(BaseFiller):
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter, resource: ResourceEnum,
                 fill_manager: FillManager):
        super().__init__(resource=resource, postgis_connector=postgis_connector, eminfra_importer=eminfra_importer,
                         updater=ControleficheUpdater(), fill_manager=fill_manager)

