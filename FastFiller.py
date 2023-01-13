import abc
from abc import ABC

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class FastFiller(ABC):
    def __init__(self, resource: str, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter):
        self.postgis_connector = postgis_connector
        self.eminfra_importer = eminfra_importer
        self.resource = resource

    @abc.abstractmethod
    def update_objects(self, object_dicts):
        raise NotImplementedError()

    def fill(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            objects = self.eminfra_importer.import_resource_from_webservice_page_by_page(resource=self.resource,
                                                                                         page_size=page_size)
            if len(list(objects)) == 0:
                break

            self.update_objects(object_dicts=objects)
            self.postgis_connector.update_params({f'{self.resource}_cursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break
