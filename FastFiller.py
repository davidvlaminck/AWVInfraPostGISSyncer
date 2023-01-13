import abc
import itertools
from abc import ABC
from typing import Iterator

from EMInfraImporter import EMInfraImporter
from PostGISConnector import PostGISConnector


class FastFiller(ABC):
    def __init__(self, resource: str, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter):
        self.postgis_connector = postgis_connector
        self.eminfra_importer = eminfra_importer
        self.resource = resource

    def peek_generator(self, iterable):
        try:
            first = next(iterable)
        except StopIteration:
            return None
        yield from itertools.chain([first], iterable)

    @abc.abstractmethod
    def update_objects(self, object_generator: Iterator[dict]):
        raise NotImplementedError()

    def fill(self, pagingcursor: str = '', page_size: int = 100):
        self.eminfra_importer.pagingcursor = pagingcursor
        while True:
            object_generator = self.eminfra_importer.import_resource_from_webservice_page_by_page(
                resource=self.resource, page_size=page_size)

            self.update_objects(object_generator=object_generator)
            self.postgis_connector.update_params({f'{self.resource}_cursor': self.eminfra_importer.pagingcursor})

            if self.eminfra_importer.pagingcursor == '':
                break
