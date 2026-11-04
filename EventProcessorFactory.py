from EMInfraImporter import EMInfraImporter
from EventProcessors.AgentProcessors.AgentOvocodeGewijzigdProcessor import AgentOvocodeGewijzigdProcessor
from EventProcessors.AssetProcessors.ActiefGewijzigdProcessor import ActiefGewijzigdProcessor
from EventProcessors.AgentProcessors.AgentActiefGewijzigdProcessor import AgentActiefGewijzigdProcessor
from EventProcessors.AgentProcessors.AgentContactInfoGewijzigdProcessor import AgentContactInfoGewijzigdProcessor
from EventProcessors.AgentProcessors.AgentNaamGewijzigdProcessor import AgentNaamGewijzigdProcessor
from EventProcessors.AgentProcessors.AgentVOIDGewijzigdProcessor import AgentVOIDGewijzigdProcessor
from EventProcessors.AgentProcessors.NieuwAgentProcessor import NieuwAgentProcessor
from EventProcessors.AssetProcessors.VplanGewijzigdProcessor import VplanGewijzigdProcessor
from EventProcessors.AssetProcessors.WeglocatieGewijzigdProcessor import WeglocatieGewijzigdProcessor
from EventProcessors.AssetrelatieProcessors.AssetrelatieEigenschappenGewijzigdProcessor import \
    AssetrelatieEigenschappenGewijzigdProcessor
from EventProcessors.AssetrelatieProcessors.AssetrelatieVerwijderdOngedaanProcessor import \
    AssetrelatieVerwijderdOngedaanProcessor
from EventProcessors.AssetrelatieProcessors.AssetrelatieVerwijderdProcessor import AssetrelatieVerwijderdProcessor
from EventProcessors.AssetrelatieProcessors.NieuwAssetrelatieProcessor import NieuwAssetrelatieProcessor
from EventProcessors.AssetProcessors.AttributenGewijzigdProcessor import AttributenGewijzigdProcessor
from EventProcessors.AssetProcessors.BestekGewijzigdProcessor import BestekGewijzigdProcessor
from EventProcessors.BetrokkenerelatieProcessors.BetrokkenerelatieContactInfoGewijzigdProcessor import \
    BetrokkenerelatieContactInfoGewijzigdProcessor
from EventProcessors.BetrokkenerelatieProcessors.BetrokkenerelatieGeldigheidGewijzigdProcessor import \
    BetrokkenerelatieGeldigheidGewijzigdProcessor
from EventProcessors.BetrokkenerelatieProcessors.BetrokkenerelatieRolGewijzigdProcessor import \
    BetrokkenerelatieRolGewijzigdProcessor
from EventProcessors.BetrokkenerelatieProcessors.BetrokkenerelatieVerwijderdOngedaanProcessor import \
    BetrokkenerelatieVerwijderdOngedaanProcessor
from EventProcessors.BetrokkenerelatieProcessors.BetrokkenerelatieVerwijderdProcessor import \
    BetrokkenerelatieVerwijderdProcessor
from EventProcessors.BetrokkenerelatieProcessors.NieuwBetrokkenerelatieProcessor import NieuwBetrokkenerelatieProcessor
from EventProcessors.AssetProcessors.CommentaarGewijzigdProcessor import CommentaarGewijzigdProcessor
from EventProcessors.AssetProcessors.GeometrieOrLocatieGewijzigdProcessor import GeometrieOrLocatieGewijzigdProcessor
from EventProcessors.AssetProcessors.NaamGewijzigdProcessor import NaamGewijzigdProcessor
from EventProcessors.AssetProcessors.NieuwAssetProcessor import NieuwAssetProcessor
from EventProcessors.AssetProcessors.SchadebeheerderGewijzigdProcessor import SchadebeheerderGewijzigdProcessor
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from EventProcessors.AssetProcessors.ToestandGewijzigdProcessor import ToestandGewijzigdProcessor
from EventProcessors.AssetProcessors.ToezichtGewijzigdProcessor import ToezichtGewijzigdProcessor
from EventProcessors.ControleficheProcessors.NieuwControleficheProcessor import NieuwControleficheProcessor
from PostGISConnector import PostGISConnector


class EventProcessorFactory:
    _RESOURCE_FACTORIES = {
        'agents': 'create_agent_event_processor',
        'betrokkenerelaties': 'create_betrokkene_relatie_event_processor',
        'assetrelaties': 'create_assetrelatie_event_processor',
        'assets': 'create_asset_event_processor',
        'controlefiches': 'create_controlefiche_event_processor',
    }

    @classmethod
    def create_event_processor(cls, event_type: str, eminfra_importer: EMInfraImporter,
                               postgis_connector: PostGISConnector, resource: str) -> SpecificEventProcessor:
        method_name = cls._RESOURCE_FACTORIES.get(resource)
        if method_name is None:
            raise NotImplementedError
        return getattr(cls, method_name)(event_type=event_type, eminfra_importer=eminfra_importer)

    _ASSET_PROCESSORS = {
        'NIEUWE_INSTALLATIE': NieuwAssetProcessor,
        'NIEUW_ONDERDEEL': NieuwAssetProcessor,
        'NIEUWE_CONTROLEFICHE': NieuwAssetProcessor,
        'NIEUWE_AANLEIDING': NieuwAssetProcessor,
        'NIEUWE_BEHEERACTIE': NieuwAssetProcessor,
        'ACTIEF_GEWIJZIGD': ActiefGewijzigdProcessor,
        'BESTEK_GEWIJZIGD': BestekGewijzigdProcessor,
        'BETROKKENE_RELATIES_GEWIJZIGD': None,
        'BETROKKENE_RELATIES_EIGENSCHAPPEN_GEWIJZIGD': None,
        'COMMENTAAR_GEWIJZIGD': CommentaarGewijzigdProcessor,
        'COMMUNICATIEAANSLUITING_GEWIJZIGD': None,
        'DOCUMENTEN_GEWIJZIGD': None,
        'EIGENSCHAPPEN_GEWIJZIGD': AttributenGewijzigdProcessor,
        'ELEKTRICITEITSAANSLUITING_GEWIJZIGD': None,
        'GEOMETRIE_GEWIJZIGD': GeometrieOrLocatieGewijzigdProcessor,
        'LOCATIE_GEWIJZIGD': GeometrieOrLocatieGewijzigdProcessor,
        'NAAM_GEWIJZIGD': NaamGewijzigdProcessor,
        'NAAMPAD_GEWIJZIGD': NaamGewijzigdProcessor,
        'PARENT_GEWIJZIGD': NaamGewijzigdProcessor,
        'POSTIT_GEWIJZIGD': None,
        'RELATIES_GEWIJZIGD': None,
        'RELATIES_EIGENSCHAPPEN_GEWIJZIGD': None,
        'SCHADEBEHEERDER_GEWIJZIGD': SchadebeheerderGewijzigdProcessor,
        'TOEGANG_GEWIJZIGD': None,
        'TOESTAND_GEWIJZIGD': ToestandGewijzigdProcessor,
        'TOEZICHT_GEWIJZIGD': ToezichtGewijzigdProcessor,
        'VPLAN_GEWIJZIGD': VplanGewijzigdProcessor,
        'WEGLOCATIE_GEWIJZIGD': WeglocatieGewijzigdProcessor,
    }

    @classmethod
    def create_asset_event_processor(cls, event_type: str, eminfra_importer: EMInfraImporter) -> SpecificEventProcessor:
        processor_class = cls._ASSET_PROCESSORS.get(event_type, _MISSING)
        if processor_class is _MISSING:
            raise NotImplementedError(f"can't create an asset event processor with type: {event_type}")
        if processor_class is None:
            return None
        return processor_class(eminfra_importer)

    _AGENT_PROCESSORS = {
        'NIEUWE_AGENT': NieuwAgentProcessor,
        'NAAM_GEWIJZIGD': AgentNaamGewijzigdProcessor,
        'VO_ID_GEWIJZIGD': AgentVOIDGewijzigdProcessor,
        'CONTACT_INFO_GEWIJZIGD': AgentContactInfoGewijzigdProcessor,
        'ACTIEF_GEWIJZIGD': AgentActiefGewijzigdProcessor,
        'OVO_CODE_GEWIJZIGD': AgentOvocodeGewijzigdProcessor,
        'BETROKKENE_RELATIES_GEWIJZIGD': None,
        'BETROKKENE_RELATIES_EIGENSCHAPPEN_GEWIJZIGD': None,
    }

    @classmethod
    def create_agent_event_processor(cls, event_type: str, eminfra_importer: EMInfraImporter) -> SpecificEventProcessor:
        processor_class = cls._AGENT_PROCESSORS.get(event_type, _MISSING)
        if processor_class is _MISSING:
            raise NotImplementedError(f"can't create an agent event processor with type: {event_type}")
        if processor_class is None:
            return None
        return processor_class(eminfra_importer=eminfra_importer)

    _BETROKKENE_PROCESSORS = {
        'NIEUWE_RELATIE': NieuwBetrokkenerelatieProcessor,
        'RELATIE_VERWIJDERD_ONGEDAAN': BetrokkenerelatieVerwijderdOngedaanProcessor,
        'RELATIE_VERWIJDERD': BetrokkenerelatieVerwijderdProcessor,
        'ROL_GEWIJZIGD': BetrokkenerelatieRolGewijzigdProcessor,
        'CONTACT_INFO_GEWIJZIGD': BetrokkenerelatieContactInfoGewijzigdProcessor,
        'GELDIGHEID_GEWIJZIGD': BetrokkenerelatieGeldigheidGewijzigdProcessor,
        'REDEN_GEWIJZIGD': None,
    }

    @classmethod
    def create_betrokkene_relatie_event_processor(cls, event_type: str, eminfra_importer: EMInfraImporter) -> \
            SpecificEventProcessor:
        processor_class = cls._BETROKKENE_PROCESSORS.get(event_type, _MISSING)
        if processor_class is _MISSING:
            raise NotImplementedError(f"can't create a betrokkenerelatie event processor with type: {event_type}")
        if processor_class is None:
            return None
        return processor_class(eminfra_importer=eminfra_importer)

    _ASSETRELATIE_PROCESSORS = {
        'NIEUWE_RELATIE': NieuwAssetrelatieProcessor,
        'RELATIE_VERWIJDERD_ONGEDAAN': AssetrelatieVerwijderdOngedaanProcessor,
        'RELATIE_VERWIJDERD': AssetrelatieVerwijderdProcessor,
        'EIGENSCHAPPEN_GEWIJZIGD': AssetrelatieEigenschappenGewijzigdProcessor,
    }

    @classmethod
    def create_assetrelatie_event_processor(cls, event_type: str, eminfra_importer: EMInfraImporter) -> \
            SpecificEventProcessor:
        processor_class = cls._ASSETRELATIE_PROCESSORS.get(event_type, _MISSING)
        if processor_class is _MISSING:
            raise NotImplementedError(f"can't create an betrokkenerelatie event processor with type: {event_type}")
        if processor_class is None:
            return None
        return processor_class(eminfra_importer=eminfra_importer)

    _CONTROLEFICHE_PROCESSORS = {
        'NIEUWE_CONTROLEFICHE': NieuwControleficheProcessor,
    }

    @classmethod
    def create_controlefiche_event_processor(cls, event_type: str, eminfra_importer: EMInfraImporter
                                             ) -> SpecificEventProcessor:
        processor_class = cls._CONTROLEFICHE_PROCESSORS.get(event_type, _MISSING)
        if processor_class is _MISSING:
            raise NotImplementedError(f"can't create an asset event processor with type: {event_type}")
        if processor_class is None:
            return None
        return processor_class(eminfra_importer)


_MISSING = object()
