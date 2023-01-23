from abc import abstractmethod

import psycopg2

from EMInfraImporter import EMInfraImporter


class SpecificEventProcessor:
    def __init__(self, cursor: psycopg2._psycopg.cursor, eminfra_importer: EMInfraImporter):
        self.em_infra_importer = eminfra_importer
        self.cursor = cursor

    @abstractmethod
    def process(self, uuids: [str]):
        pass


"""
Event types agents
Agent event type
	
NIEUWE_AGENTS
Een nieuwe agent werd toegevoegd.

NAAM_GEWIJZIGD
De naam van een agent werd gewijzigd.

VO_ID_GEWIJZIGD
De vo-id van een agent werd gewijzigd.

CONTACT_INFO_GEWIJZIGD
De contact info van een agent werd gewijzigd.

ACTIEF_GEWIJZIGD
De actief vlag van een agent werd gewijzigd.


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

Event types betrokkenerelaties
------------------------------
NIEUWE_RELATIE
Een nieuwe relatie werd toegevoegd.

RELATIE_VERWIJDERD
Een relatie werd gewijzigd. (van actief ⇒ non actief in het OTL model)

RELATIE_VERWIJDERD_ONGEDAAN
Verwijderen van een relatie werd ongedaan gemaakt (van nonactief ⇒ actief in het OTL model)

CONTACT_INFO_GEWIJZIGD
De contact info van een relatie werd gewijzigd.

ROL_GEWIJZIGD
De rol van een relatie werd gewijzigd.

GELDIGHEID_GEWIJZIGD
De geldigheid van een relatie werd gewijzigd.
"""