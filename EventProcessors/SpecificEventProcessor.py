from abc import abstractmethod

import psycopg2

from EMInfraImporter import EMInfraImporter


class SpecificEventProcessor:
    def __init__(self, cursor: psycopg2._psycopg.cursor, em_infra_importer: EMInfraImporter):
        self.em_infra_importer = em_infra_importer
        self.cursor = cursor

    @abstractmethod
    def process(self, uuids: [str]):
        pass
