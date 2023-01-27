from EMInfraImporter import EMInfraImporter
from EventProcessors.AssetProcessors.ActiefGewijzigdProcessor import ActiefGewijzigdProcessor
from EventProcessors.AgentProcessors.AgentActiefGewijzigdProcessor import AgentActiefGewijzigdProcessor
from EventProcessors.AgentProcessors.AgentContactInfoGewijzigdProcessor import AgentContactInfoGewijzigdProcessor
from EventProcessors.AgentProcessors.AgentNaamGewijzigdProcessor import AgentNaamGewijzigdProcessor
from EventProcessors.AgentProcessors.AgentVOIDGewijzigdProcessor import AgentVOIDGewijzigdProcessor
from EventProcessors.AgentProcessors.NieuwAgentProcessor import NieuwAgentProcessor
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
from EventProcessors.AssetProcessors.ElekAansluitingGewijzigdProcessor import ElekAansluitingGewijzigdProcessor
from EventProcessors.AssetProcessors.GeometrieOrLocatieGewijzigdProcessor import GeometrieOrLocatieGewijzigdProcessor
from EventProcessors.AssetProcessors.NaamGewijzigdProcessor import NaamGewijzigdProcessor
from EventProcessors.AssetProcessors.NieuwAssetProcessor import NieuwAssetProcessor
from EventProcessors.AssetProcessors.SchadebeheerderGewijzigdProcessor import SchadebeheerderGewijzigdProcessor
from EventProcessors.AssetProcessors.SpecificEventProcessor import SpecificEventProcessor
from EventProcessors.AssetProcessors.ToestandGewijzigdProcessor import ToestandGewijzigdProcessor
from EventProcessors.AssetProcessors.ToezichtGewijzigdProcessor import ToezichtGewijzigdProcessor
from PostGISConnector import PostGISConnector


class EventProcessorFactory:
    @classmethod
    def create_event_processor(cls, event_type: str, eminfra_importer: EMInfraImporter,
                               postgis_connector: PostGISConnector, resource: str) -> SpecificEventProcessor:
        if resource == 'agents':
            return EventProcessorFactory.create_agent_event_processor(event_type=event_type,
                                                                      eminfra_importer=eminfra_importer)
        elif resource == 'betrokkenerelaties':
            return EventProcessorFactory.create_betrokkene_relatie_event_processor(
                event_type=event_type, eminfra_importer=eminfra_importer)
        elif resource == 'assetrelaties':
            return EventProcessorFactory.create_assetrelatie_event_processor(
                event_type=event_type, eminfra_importer=eminfra_importer)
        elif resource == 'assets':
            return EventProcessorFactory.create_asset_event_processor(
                event_type=event_type, eminfra_importer=eminfra_importer)

        raise NotImplementedError

    @classmethod
    def create_asset_event_processor(cls, event_type: str, eminfra_importer: EMInfraImporter) -> SpecificEventProcessor:
        if event_type == 'NIEUWE_INSTALLATIE':
            return NieuwAssetProcessor(eminfra_importer)
        elif event_type == 'NIEUW_ONDERDEEL':
            return NieuwAssetProcessor(eminfra_importer)
        elif event_type == 'ACTIEF_GEWIJZIGD':
            return ActiefGewijzigdProcessor(eminfra_importer)
        elif event_type == 'BESTEK_GEWIJZIGD':
            return BestekGewijzigdProcessor(eminfra_importer)
        elif event_type == 'BETROKKENE_RELATIES_GEWIJZIGD':
            pass  # using different feed instead
        elif event_type == 'COMMENTAAR_GEWIJZIGD':
            return CommentaarGewijzigdProcessor(eminfra_importer)
        elif event_type == 'COMMUNICATIEAANSLUITING_GEWIJZIGD':
            pass
        elif event_type == 'DOCUMENTEN_GEWIJZIGD':
            pass
        elif event_type == 'EIGENSCHAPPEN_GEWIJZIGD':
            return AttributenGewijzigdProcessor(eminfra_importer)
        elif event_type == 'ELEKTRICITEITSAANSLUITING_GEWIJZIGD':
            return ElekAansluitingGewijzigdProcessor(eminfra_importer)
        elif event_type == 'GEOMETRIE_GEWIJZIGD' or event_type == 'LOCATIE_GEWIJZIGD':
            return GeometrieOrLocatieGewijzigdProcessor(eminfra_importer)
        elif event_type == 'NAAM_GEWIJZIGD' or event_type == 'NAAMPAD_GEWIJZIGD' or event_type == 'PARENT_GEWIJZIGD':
            return NaamGewijzigdProcessor(eminfra_importer)
        elif event_type == 'POSTIT_GEWIJZIGD':
            pass
        elif event_type == 'RELATIES_GEWIJZIGD':
            pass  # using different feed instead
        elif event_type == 'SCHADEBEHEERDER_GEWIJZIGD':
            return SchadebeheerderGewijzigdProcessor(eminfra_importer)
        elif event_type == 'TOEGANG_GEWIJZIGD':
            pass
        elif event_type == 'TOESTAND_GEWIJZIGD':
            return ToestandGewijzigdProcessor(eminfra_importer)
        elif event_type == 'TOEZICHT_GEWIJZIGD':
            return ToezichtGewijzigdProcessor(eminfra_importer)
        elif event_type == 'VPLAN_GEWIJZIGD':
            pass
        else:
            raise NotImplementedError(f"can't create an asset event processor with type: {event_type}")

    @classmethod
    def create_agent_event_processor(cls, event_type: str, eminfra_importer: EMInfraImporter) -> SpecificEventProcessor:
        if event_type == 'NIEUWE_AGENT':
            return NieuwAgentProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'NAAM_GEWIJZIGD':
            return AgentNaamGewijzigdProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'VO_ID_GEWIJZIGD':
            return AgentVOIDGewijzigdProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'CONTACT_INFO_GEWIJZIGD':
            return AgentContactInfoGewijzigdProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'ACTIEF_GEWIJZIGD':
            return AgentActiefGewijzigdProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'BETROKKENE_RELATIES_GEWIJZIGD':
            pass  # using different feed instead
        else:
            raise NotImplementedError(f"can't create an agent event processor with type: {event_type}")

    @classmethod
    def create_betrokkene_relatie_event_processor(cls, event_type: str, eminfra_importer: EMInfraImporter) -> \
            SpecificEventProcessor:
        if event_type == 'NIEUWE_RELATIE':
            return NieuwBetrokkenerelatieProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'RELATIE_VERWIJDERD_ONGEDAAN':
            return BetrokkenerelatieVerwijderdOngedaanProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'RELATIE_VERWIJDERD':
            return BetrokkenerelatieVerwijderdProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'ROL_GEWIJZIGD':
            return BetrokkenerelatieRolGewijzigdProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'CONTACT_INFO_GEWIJZIGD':
            return BetrokkenerelatieContactInfoGewijzigdProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'GELDIGHEID_GEWIJZIGD':
            return BetrokkenerelatieGeldigheidGewijzigdProcessor(eminfra_importer=eminfra_importer)
        else:
            raise NotImplementedError(f"can't create an betrokkenerelatie event processor with type: {event_type}")

    @classmethod
    def create_assetrelatie_event_processor(cls, event_type: str, eminfra_importer: EMInfraImporter) -> \
            SpecificEventProcessor:
        if event_type == 'NIEUWE_RELATIE':
            return NieuwAssetrelatieProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'RELATIE_VERWIJDERD_ONGEDAAN':
            return AssetrelatieVerwijderdOngedaanProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'RELATIE_VERWIJDERD':
            return AssetrelatieVerwijderdProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'EIGENSCHAPPEN_GEWIJZIGD':
            return AssetrelatieEigenschappenGewijzigdProcessor(eminfra_importer=eminfra_importer)
        else:
            raise NotImplementedError(f"can't create an betrokkenerelatie event processor with type: {event_type}")
