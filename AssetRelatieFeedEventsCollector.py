from EMInfraImporter import EMInfraImporter
from FeedEventsCollector import FeedEventsCollector


class AssetRelatieFeedEventsCollector(FeedEventsCollector):
    def __init__(self, eminfra_importer: EMInfraImporter):
        super().__init__(eminfra_importer)
        self.resource = 'assetrelaties'

    @staticmethod
    def create_empty_event_dict() -> {}:
        return {event_type: set() for event_type in
                ['NIEUWE_RELATIE', 'RELATIE_VERWIJDERD', 'RELATIE_VERWIJDERD_ONGEDAAN', 'EIGENSCHAPPEN_GEWIJZIGD']}
