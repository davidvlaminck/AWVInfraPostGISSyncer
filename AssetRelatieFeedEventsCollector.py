from EMInfraImporter import EMInfraImporter
from FeedEventsCollector import FeedEventsCollector


class AssetRelatieFeedEventsCollector(FeedEventsCollector):
    def __init__(self, eminfra_importer: EMInfraImporter):
        super().__init__(eminfra_importer)
        self.resource = 'assetrelaties'

    @staticmethod
    def create_empty_event_dict() -> {}:
        empty_dict = {}
        for event_type in ['NIEUWE_RELATIE', 'RELATIE_VERWIJDERD', 'RELATIE_VERWIJDERD_ONGEDAAN',
                           'EIGENSCHAPPEN_GEWIJZIGD']:
            empty_dict[event_type] = set()
        return empty_dict
