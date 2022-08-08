from EMInfraImporter import EMInfraImporter
from EventProcessors.ActiefGewijzigdProcessor import ActiefGewijzigdProcessor
from EventProcessors.AssetRelatiesGewijzigdProcessor import AssetRelatiesGewijzigdProcessor
from EventProcessors.BetrokkeneRelatiesGewijzigdProcessor import BetrokkeneRelatiesGewijzigdProcessor
from EventProcessors.CommentaarGewijzigdProcessor import CommentaarGewijzigdProcessor
from EventProcessors.EigenschappenGewijzigdProcessor import EigenschappenGewijzigdProcessor
from EventProcessors.GeometrieOrLocatieGewijzigdProcessor import GeometrieOrLocatieGewijzigdProcessor
from EventProcessors.NaamGewijzigdProcessor import NaamGewijzigdProcessor
from EventProcessors.NieuwAssetProcessor import NieuwAssetProcessor
from EventProcessors.SchadebeheerderGewijzigdProcessor import SchadebeheerderGewijzigdProcessor
from EventProcessors.SpecificEventProcessor import SpecificEventProcessor
from EventProcessors.ToestandGewijzigdProcessor import ToestandGewijzigdProcessor
from EventProcessors.ToezichtGewijzigdProcessor import ToezichtGewijzigdProcessor


class EventProcessorFactory:
    @classmethod
    def create_event_processor(cls, event_type: str, cursor,
                               em_infra_importer: EMInfraImporter) -> SpecificEventProcessor:
        if event_type == 'NIEUWE_INSTALLATIE':
            return NieuwAssetProcessor(cursor, em_infra_importer)
        elif event_type == 'NIEUW_ONDERDEEL':
            return NieuwAssetProcessor(cursor, em_infra_importer)
        elif event_type == 'ACTIEF_GEWIJZIGD':
            return ActiefGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'BESTEK_GEWIJZIGD':
            raise NotImplementedError
        elif event_type == 'BETROKKENE_RELATIES_GEWIJZIGD':
            return BetrokkeneRelatiesGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'COMMENTAAR_GEWIJZIGD':
            return CommentaarGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'COMMUNICATIEAANSLUITING_GEWIJZIGD':
            raise NotImplementedError
        elif event_type == 'DOCUMENTEN_GEWIJZIGD':
            raise NotImplementedError
        elif event_type == 'EIGENSCHAPPEN_GEWIJZIGD':
            return EigenschappenGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'ELEKTRICITEITSAANSLUITING_GEWIJZIGD':
            raise NotImplementedError
        elif event_type == 'GEOMETRIE_GEWIJZIGD' or event_type == 'LOCATIE_GEWIJZIGD':
            return GeometrieOrLocatieGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'NAAM_GEWIJZIGD' or event_type == 'NAAMPAD_GEWIJZIGD' or event_type == 'PARENT_GEWIJZIGD':
            return NaamGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'POSTIT_GEWIJZIGD':
            raise NotImplementedError
        elif event_type == 'RELATIES_GEWIJZIGD':
            return AssetRelatiesGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'SCHADEBEHEERDER_GEWIJZIGD':
            return SchadebeheerderGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'TOEGANG_GEWIJZIGD':
            raise NotImplementedError
        elif event_type == 'TOESTAND_GEWIJZIGD':
            return ToestandGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'TOEZICHT_GEWIJZIGD':
            return ToezichtGewijzigdProcessor(cursor, em_infra_importer)
        elif event_type == 'VPLAN_GEWIJZIGD':
            raise NotImplementedError
        else:
            raise NotImplementedError
