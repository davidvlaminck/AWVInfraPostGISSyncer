from abc import ABC, abstractmethod



from EMInfraImporter import EMInfraImporter


class SpecificEventProcessor(ABC):
    def __init__(self, tx_context, emInfraImporter: EMInfraImporter):
        self.tx_context = tx_context
        self.emInfraImporter = emInfraImporter

    @abstractmethod
    def process(self, uuids: [str]):
        pass
