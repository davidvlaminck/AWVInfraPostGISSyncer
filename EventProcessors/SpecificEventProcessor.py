from abc import abstractmethod

import psycopg2

from EMInfraImporter import EMInfraImporter


class SpecificEventProcessor:
    def __init__(self, eminfra_importer: EMInfraImporter):
        self.em_infra_importer = eminfra_importer

    @abstractmethod
    def process(self, uuids: [str], connection):
        pass


"""

https://apps-tei.mow.vlaanderen.be/eminfra/feedproxy/feed/assetrelaties
https://apps-tei.mow.vlaanderen.be/eminfra/feedproxy/feed/betrokkenerelaties

Event types assetrelaties
-------------------------
NIEUWE_RELATIE
Een nieuwe relatie werd toegevoegd.

RELATIE_VERWIJDERD
Een relatie werd gewijzigd. (van actief ⇒ non actief in het OTL model)

RELATIE_VERWIJDERD_ONGEDAAN
Verwijderen van een relatie werd ongedaan gemaakt (van nonactief ⇒ actief in het OTL model)

EIGENSCHAPPEN_GEWIJZIGD
De eigenschappen van een relatie werden gewijzigd.
"""