from EMInfraImporter import EMInfraImporter
from FeedEventsCollector import FeedEventsCollector


class AssetFeedEventsCollector(FeedEventsCollector):
    def __init__(self, eminfra_importer: EMInfraImporter):
        super().__init__(eminfra_importer)
        self.resource = 'assets'

    @staticmethod
    def create_empty_event_dict() -> {}:
        empty_dict = {}
        for event_type in ['ACTIEF_GEWIJZIGD', 'BESTEK_GEWIJZIGD', 'BETROKKENE_RELATIES_GEWIJZIGD',
                           'COMMENTAAR_GEWIJZIGD', 'COMMUNICATIEAANSLUITING_GEWIJZIGD', 'DOCUMENTEN_GEWIJZIGD',
                           'EIGENSCHAPPEN_GEWIJZIGD', 'ELEKTRICITEITSAANSLUITING_GEWIJZIGD', 'GEOMETRIE_GEWIJZIGD',
                           'LOCATIE_GEWIJZIGD', 'NAAM_GEWIJZIGD', 'NAAMPAD_GEWIJZIGD', 'NIEUW_ONDERDEEL',
                           'NIEUWE_INSTALLATIE', 'PARENT_GEWIJZIGD', 'POSTIT_GEWIJZIGD', 'RELATIES_GEWIJZIGD',
                           'SCHADEBEHEERDER_GEWIJZIGD', 'TOEGANG_GEWIJZIGD', 'TOESTAND_GEWIJZIGD', 'TOEZICHT_GEWIJZIGD',
                           'VPLAN_GEWIJZIGD']:
            empty_dict[event_type] = set()
        return empty_dict