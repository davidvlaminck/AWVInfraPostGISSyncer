from EMInfraImporter import EMInfraImporter
from EventProcessors.BetrokkeneRelatiesGewijzigdProcessor import BetrokkeneRelatiesGewijzigdProcessor
from PostGISConnector import PostGISConnector


class BetrokkeneRelatiesSyncer:
    def __init__(self, post_gis_connector: PostGISConnector, em_infra_importer: EMInfraImporter):
        self.postGIS_connector = post_gis_connector
        self.eminfra_importer = em_infra_importer

    def sync_betrokkenerelaties(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            cursor = self.postGIS_connector.connection.cursor()
            processor = BetrokkeneRelatiesGewijzigdProcessor(cursor=cursor, em_infra_importer=self.eminfra_importer,
                                                             connector=self.postGIS_connector)
            relaties = self.eminfra_importer.import_betrokkenerelaties_from_webservice_page_by_page(page_size=page_size)
            if len(relaties) == 0:
                break

            asset_uuids = list(set(map(lambda x: x['bron']['uuid'], relaties)))
            processor.process_dicts(betrokkenerelatie_dicts=relaties, cursor=cursor, asset_uuids=asset_uuids)
            self.postGIS_connector.save_props_to_params(cursor=cursor,
                                                        params={'pagingcursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break
