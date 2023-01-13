from abc import ABC

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class FastFiller(ABC):
    def __init__(self, resource: str, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter, updater):
        self.postgis_connector = postgis_connector
        self.eminfra_importer = eminfra_importer
        self.resource = resource
        self.updater = updater

    def fill(self, connection, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.paging_cursors[self.resource] = pagingcursor
        while True:
            object_generator = self.eminfra_importer.import_resource_from_webservice_page_by_page(
                resource=self.resource, page_size=page_size)

            self.updater.update_objects(object_generator=object_generator, connection=connection)
            self.postgis_connector.update_params(
                params={f'{self.resource}_cursor': self.eminfra_importer.paging_cursors[self.resource]},
                connection=connection)

            connection.commit()

            if self.eminfra_importer.paging_cursors[self.resource] == '':
                break
