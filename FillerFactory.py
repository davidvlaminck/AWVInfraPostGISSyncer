import importlib

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector
from ResourceEnum import ResourceEnum

FILLER_FACTORY_DICT = {
    ResourceEnum.agents: 'AgentFiller',
    ResourceEnum.assets: 'AssetFiller',
    ResourceEnum.identiteiten: 'IdentiteitFiller',
    ResourceEnum.assettypes: 'AssetTypeFiller',
    ResourceEnum.relatietypes: 'RelatieTypeFiller',
    ResourceEnum.bestekken: 'BestekFiller',
    ResourceEnum.toezichtgroepen: 'ToezichtgroepFiller',
    ResourceEnum.beheerders: 'BeheerderFiller',
    ResourceEnum.assetrelaties: 'AssetRelatieFiller',
    ResourceEnum.betrokkenerelaties: 'BetrokkeneRelatieFiller',
}


class FillerFactory:
    @staticmethod
    def create_filler(eminfra_importer: EMInfraImporter, resource: ResourceEnum, postgis_connector: PostGISConnector,
                      fill_manager):
        name = FILLER_FACTORY_DICT[resource]
        module = importlib.import_module(name)
        class_ = getattr(module, name)
        instance = class_(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                          resource=resource, fill_manager=fill_manager)
        return instance
