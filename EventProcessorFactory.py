from EMInfraImporter import EMInfraImporter
from EventProcessors.ActiefGewijzigdProcessor import ActiefGewijzigdProcessor
from EventProcessors.AgentProcessors.AgentActiefGewijzigdProcessor import AgentActiefGewijzigdProcessor
from EventProcessors.AgentProcessors.AgentContactInfoGewijzigdProcessor import AgentContactInfoGewijzigdProcessor
from EventProcessors.AgentProcessors.AgentNaamGewijzigdProcessor import AgentNaamGewijzigdProcessor
from EventProcessors.AgentProcessors.AgentVOIDGewijzigdProcessor import AgentVOIDGewijzigdProcessor
from EventProcessors.AssetRelatiesGewijzigdProcessor import AssetRelatiesGewijzigdProcessor
from EventProcessors.AttributenGewijzigdProcessor import AttributenGewijzigdProcessor
from EventProcessors.BestekGewijzigdProcessor import BestekGewijzigdProcessor
from EventProcessors.BetrokkeneRelatiesGewijzigdProcessor import BetrokkeneRelatiesGewijzigdProcessor
from EventProcessors.CommentaarGewijzigdProcessor import CommentaarGewijzigdProcessor
from EventProcessors.ElekAansluitingGewijzigdProcessor import ElekAansluitingGewijzigdProcessor
from EventProcessors.GeometrieOrLocatieGewijzigdProcessor import GeometrieOrLocatieGewijzigdProcessor
from EventProcessors.NaamGewijzigdProcessor import NaamGewijzigdProcessor
from EventProcessors.AgentProcessors.NieuwAgentProcessor import NieuwAgentProcessor
from EventProcessors.NieuwAssetProcessor import NieuwAssetProcessor
from EventProcessors.SchadebeheerderGewijzigdProcessor import SchadebeheerderGewijzigdProcessor
from EventProcessors.SpecificEventProcessor import SpecificEventProcessor
from EventProcessors.ToestandGewijzigdProcessor import ToestandGewijzigdProcessor
from EventProcessors.ToezichtGewijzigdProcessor import ToezichtGewijzigdProcessor
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

        raise NotImplementedError

        if event_type == 'NIEUWE_INSTALLATIE':
            return NieuwAssetProcessor(cursor, em_infra_importer)
        elif event_type == 'NIEUW_ONDERDEEL':
            return NieuwAssetProcessor(cursor, em_infra_importer)
        elif event_type == 'ACTIEF_GEWIJZIGD':
            return ActiefGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'BESTEK_GEWIJZIGD':
            return BestekGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'BETROKKENE_RELATIES_GEWIJZIGD':
            return BetrokkeneRelatiesGewijzigdProcessor(cursor=cursor, eminfra_importer=em_infra_importer,
                                                        connector=postgis_connector)
        elif event_type == 'COMMENTAAR_GEWIJZIGD':
            return CommentaarGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'COMMUNICATIEAANSLUITING_GEWIJZIGD':
            pass
        elif event_type == 'DOCUMENTEN_GEWIJZIGD':
            pass
        elif event_type == 'EIGENSCHAPPEN_GEWIJZIGD':
            return AttributenGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'ELEKTRICITEITSAANSLUITING_GEWIJZIGD':
            return ElekAansluitingGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'GEOMETRIE_GEWIJZIGD' or event_type == 'LOCATIE_GEWIJZIGD':
            return GeometrieOrLocatieGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'NAAM_GEWIJZIGD' or event_type == 'NAAMPAD_GEWIJZIGD' or event_type == 'PARENT_GEWIJZIGD':
            return NaamGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'POSTIT_GEWIJZIGD':
            pass
        elif event_type == 'RELATIES_GEWIJZIGD':
            return AssetRelatiesGewijzigdProcessor(cursor=cursor, eminfra_importer=em_infra_importer,
                                                   connector=postgis_connector)
        elif event_type == 'SCHADEBEHEERDER_GEWIJZIGD':
            return SchadebeheerderGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'TOEGANG_GEWIJZIGD':
            pass
        elif event_type == 'TOESTAND_GEWIJZIGD':
            return ToestandGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'TOEZICHT_GEWIJZIGD':
            return ToezichtGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'VPLAN_GEWIJZIGD':
            pass
        else:
            raise NotImplementedError()

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
        else:
            raise NotImplementedError(f"can't create an agent event processor with type: {event_type}")

    @classmethod
    def create_betrokkene_relatie_event_processor(cls, event_type: str, eminfra_importer: EMInfraImporter) -> \
            SpecificEventProcessor:
        if event_type == 'NIEUWE_RELATIE':
            raise NotImplementedError
            return NieuwAgentProcessor(eminfra_importer=eminfra_importer)
        elif event_type in ['RELATIE_VERWIJDERD', 'RELATIE_VERWIJDERD_ONGEDAAN']:
            raise NotImplementedError
            return AgentNaamGewijzigdProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'ROL_GEWIJZIGD':
            raise NotImplementedError
            return AgentVOIDGewijzigdProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'CONTACT_INFO_GEWIJZIGD':
            raise NotImplementedError
            return AgentContactInfoGewijzigdProcessor(eminfra_importer=eminfra_importer)
        elif event_type == 'GELDIGHEID_GEWIJZIGD':
            raise NotImplementedError
            return AgentActiefGewijzigdProcessor(eminfra_importer=eminfra_importer)
        else:
            raise NotImplementedError(f"can't create an agent event processor with type: {event_type}")
