from typing import Iterator

from EMInfraImporter import EMInfraImporter
from EventProcessors.BetrokkeneRelatiesGewijzigdProcessor import BetrokkeneRelatiesGewijzigdProcessor
from FastFiller import FastFiller
from Helpers import peek_generator
from PostGISConnector import PostGISConnector


class BetrokkeneRelatieSyncer(FastFiller):
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter, resource: str):
        super().__init__(resource=resource, postgis_connector=postgis_connector, eminfra_importer=eminfra_importer)

    def update_objects(self, object_generator: Iterator[dict], connection):
        object_generator = peek_generator(object_generator)
        if object_generator is None:
            return

        processor = BetrokkeneRelatiesGewijzigdProcessor(cursor=None, eminfra_importer=self.eminfra_importer,
                                                         connector=self.postgis_connector)
        processor.process_dicts(betrokkenerelatie_dicts=object_generator, connection=connection)

    def sync_betrokkenerelaties(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            cursor = self.postgis_connector.connection.cursor()
            processor = BetrokkeneRelatiesGewijzigdProcessor(cursor=cursor, eminfra_importer=self.eminfra_importer,
                                                             connector=self.postgis_connector)
            relaties = self.eminfra_importer.import_betrokkenerelaties_from_webservice_page_by_page(page_size=page_size)

            processor.process_dicts(betrokkenerelatie_dicts=relaties, cursor=cursor)
            self.postgis_connector.save_props_to_params(cursor=cursor,
                                                        params={'pagingcursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break
