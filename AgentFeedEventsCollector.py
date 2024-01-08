from EMInfraImporter import EMInfraImporter
from FeedEventsCollector import FeedEventsCollector


class AgentFeedEventsCollector(FeedEventsCollector):
    def __init__(self, eminfra_importer: EMInfraImporter):
        super().__init__(eminfra_importer)
        self.resource = 'agents'

    @staticmethod
    def create_empty_event_dict() -> {}:
        return {event_type: set() for event_type in
                ['NIEUWE_AGENT', 'NAAM_GEWIJZIGD', 'VO_ID_GEWIJZIGD', 'CONTACT_INFO_GEWIJZIGD', 'ACTIEF_GEWIJZIGD',
                 'BETROKKENE_RELATIES_GEWIJZIGD', 'OVO_CODE_GEWIJZIGD']}
