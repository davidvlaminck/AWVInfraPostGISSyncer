from AgentFiller import AgentFiller
from AssetFiller import AssetFiller
from AssetRelatieFiller import AssetRelatieFiller
from AssetTypeFiller import AssetTypeFiller
from BeheerderFiller import BeheerderFiller
from BestekFiller import BestekFiller
from BetrokkeneRelatieFiller import BetrokkeneRelatieFiller
from EMInfraImporter import EMInfraImporter
from IdentiteitFiller import IdentiteitFiller
from PostGISConnector import PostGISConnector
from RelatieTypeFiller import RelatieTypeFiller
from ToezichtgroepFiller import ToezichtgroepFiller


class FillerFactory:
    @staticmethod
    def CreateFiller(eminfra_importer: EMInfraImporter, resource: str, postgis_connector: PostGISConnector):
        if resource == 'toezichtgroepen':
            return ToezichtgroepFiller(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                                       resource=resource)
        elif resource == 'identiteiten':
            return IdentiteitFiller(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                                    resource=resource)
        elif resource == 'assettypes':
            return AssetTypeFiller(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                                   resource=resource)
        elif resource == 'relatietypes':
            return RelatieTypeFiller(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                                     resource=resource)
        elif resource == 'bestekken':
            return BestekFiller(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                                resource=resource)
        elif resource == 'beheerders':
            return BeheerderFiller(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                                   resource=resource)
        elif resource == 'agents':
            return AgentFiller(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                               resource=resource)
        elif resource == 'betrokkenerelaties':
            return BetrokkeneRelatieFiller(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                                           resource=resource)
        elif resource == 'assetrelaties':
            return AssetRelatieFiller(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                                      resource=resource)
        elif resource == 'assets':
            return AssetFiller(eminfra_importer=eminfra_importer, postgis_connector=postgis_connector,
                               resource=resource)
