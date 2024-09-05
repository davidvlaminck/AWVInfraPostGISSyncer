from EMInfraImporter import EMInfraImporter
from FeedEventsProcessor import FeedEventsProcessor
from PostGISConnector import PostGISConnector


class BetrokkeneRelatieFeedEventsProcessor(FeedEventsProcessor):
    def __init__(self, postgis_connector: PostGISConnector, eminfra_importer: EMInfraImporter):
        super().__init__(postgis_connector=postgis_connector, eminfra_importer=eminfra_importer)
        self.resource = 'betrokkenerelaties'
