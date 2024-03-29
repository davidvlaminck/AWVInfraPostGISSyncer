from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetRelatiesGewijzigdProcessor import AssetRelatiesGewijzigdProcessor
from PostGISConnector import PostGISConnector


class AssetRelatiesSyncer:
    def __init__(self, post_gis_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postGIS_connector = post_gis_connector
        self.eminfra_importer = em_infra_importer

    def sync_assetrelaties(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            cursor = self.postGIS_connector.connection.cursor()
            processor = AssetRelatiesGewijzigdProcessor(cursor=cursor, em_infra_importer=self.eminfra_importer,
                                                        connector=self.postGIS_connector)
            relaties = self.eminfra_importer.import_assetrelaties_from_webservice_page_by_page(page_size=page_size)
            if len(relaties) == 0:
                break

            #asset_uuids = list(set(map(lambda x: x['RelatieObject.bron']['@id'].replace('https://data.awvvlaanderen.be/id/asset/','')[0:36], relaties)))
            processor.process_dicts(assetrelatie_dicts=relaties, cursor=cursor)
            self.postGIS_connector.save_props_to_params(cursor=cursor,
                                                        params={'pagingcursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break
