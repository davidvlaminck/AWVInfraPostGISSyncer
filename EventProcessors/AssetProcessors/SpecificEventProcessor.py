from abc import abstractmethod

from EMInfraImporter import EMInfraImporter


class SpecificEventProcessor:
    def __init__(self, eminfra_importer: EMInfraImporter):
        self.eminfra_importer = eminfra_importer

    @abstractmethod
    def process(self, uuids: [str], connection):
        pass
