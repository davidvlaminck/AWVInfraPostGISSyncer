-- Table: public.agents

DROP TABLE IF EXISTS public.agents CASCADE;
CREATE TABLE public.agents
(
    uuid uuid NOT NULL,
    naam text,
    void uuid,
    actief boolean NOT NULL,
    contact_info json,
    PRIMARY KEY (uuid)
);

-- Table: public.params

DROP TABLE IF EXISTS public.params CASCADE;
CREATE TABLE IF NOT EXISTS public.params
(
    key_name VARCHAR(40),
    value_int integer,
    value_text text,
    value_bool boolean,
    value_timestamp TIMESTAMP WITH TIME ZONE,
    CONSTRAINT params_pkey PRIMARY KEY (key_name)
);

INSERT INTO public.params(key_name, value_bool)
VALUES ('fresh_start', TRUE);
INSERT INTO public.params(key_name, value_int)
VALUES ('pagesize', 100);

-- Table: public.assets

DROP TABLE IF EXISTS public.assets CASCADE;
CREATE TABLE IF NOT EXISTS public.assets
(
    uuid uuid NOT NULL,
    assettype uuid NOT NULL,
    toestand text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    naampad text COLLATE pg_catalog."default",
    naam text COLLATE pg_catalog."default",
    parent uuid,
    schadebeheerder uuid,
    toezichter uuid,
    toezichtgroep uuid,
    commentaar text COLLATE pg_catalog."default",
    CONSTRAINT assets_pkey PRIMARY KEY (uuid)
);

-- Table: public.assettypes

DROP TABLE IF EXISTS public.assettypes CASCADE;
CREATE TABLE IF NOT EXISTS public.assettypes
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default" NOT NULL,
    label text COLLATE pg_catalog."default" NOT NULL,
    uri text COLLATE pg_catalog."default" NOT NULL,
    definitie text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    bestek boolean,
    geometrie boolean,
    locatie boolean,
    beheerder boolean,
    toezicht boolean,
    gevoedDoor boolean,
    elek_aansluiting boolean,
    vplan boolean,
    attributen boolean,
    CONSTRAINT assettypes_pkey PRIMARY KEY (uuid)
);

ALTER TABLE IF EXISTS public.assets
    ADD CONSTRAINT assets_assettypes_fkey
    FOREIGN KEY (assettype)
    REFERENCES public.assettypes (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

-- Table: public.attributen

DROP TABLE IF EXISTS public.attributen CASCADE;
CREATE TABLE IF NOT EXISTS public.attributen
(
    uuid uuid NOT NULL,
    actief boolean NOT NULL,
    uri text COLLATE pg_catalog."default",
    naam text COLLATE pg_catalog."default",
    label text COLLATE pg_catalog."default",
    definitie text COLLATE pg_catalog."default",
    categorie text COLLATE pg_catalog."default",
    datatypeNaam text COLLATE pg_catalog."default",
    datatypeType text COLLATE pg_catalog."default",
    kardinaliteitMin text COLLATE pg_catalog."default",
    kardinaliteitMax text COLLATE pg_catalog."default",
    CONSTRAINT attributen_pkey PRIMARY KEY (uuid)
);

-- Table: public.attribuutKoppelingen

DROP TABLE IF EXISTS public.attribuutKoppelingen CASCADE;
CREATE TABLE IF NOT EXISTS public.attribuutKoppelingen
(
    assettypeUuid uuid NOT NULL,
    attribuutUuid uuid NOT NULL,
    actief boolean NOT NULL
);

ALTER TABLE IF EXISTS public.attribuutKoppelingen
    ADD CONSTRAINT assets_attribuutKoppelingen_fkey
    FOREIGN KEY (assettypeUuid)
    REFERENCES public.assettypes (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.attribuutKoppelingen
    ADD CONSTRAINT attributen_attribuutKoppelingen_fkey
    FOREIGN KEY (attribuutUuid)
    REFERENCES public.attributen (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

CREATE INDEX attribuutKoppelingen_assettypeUuid_idx ON attribuutKoppelingen (assettypeUuid);
CREATE INDEX attribuutKoppelingen_attribuutUuid_idx ON attribuutKoppelingen (attribuutUuid);

-- Table: public.attribuutWaarden

DROP TABLE IF EXISTS public.attribuutWaarden CASCADE;
CREATE TABLE IF NOT EXISTS public.attribuutWaarden
(
    assetUuid uuid NOT NULL,
    attribuutUuid uuid NOT NULL,
    waarde text COLLATE pg_catalog."default"
);

ALTER TABLE IF EXISTS public.attribuutWaarden
    ADD CONSTRAINT assets_attribuutWaarden_fkey
    FOREIGN KEY (assetUuid)
    REFERENCES public.assets (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.attribuutWaarden
    ADD CONSTRAINT attributen_attribuutWaarden_fkey
    FOREIGN KEY (attribuutUuid)
    REFERENCES public.attributen (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

CREATE INDEX attribuutWaarden_assetUuid_idx ON attribuutWaarden (assetUuid);
CREATE INDEX attribuutWaarden_attribuutUuid_idx ON attribuutWaarden (attribuutUuid);

-- Table: public.bestekken

DROP TABLE IF EXISTS public.bestekken CASCADE;
CREATE TABLE IF NOT EXISTS public.bestekken
(
    uuid uuid NOT NULL,
    eDeltaDossiernummer text COLLATE pg_catalog."default" NOT NULL,
    eDeltaBesteknummer text COLLATE pg_catalog."default" NOT NULL,
    aannemerNaam text COLLATE pg_catalog."default",
    CONSTRAINT bestekken_pkey PRIMARY KEY (uuid)
);

-- Table: public.bestekkoppelingen

DROP TABLE IF EXISTS public.bestekkoppelingen CASCADE;
CREATE TABLE IF NOT EXISTS public.bestekkoppelingen
(
    assetUuid uuid NOT NULL,
    bestekUuid uuid NOT NULL,
    startDatum TIMESTAMP WITH TIME ZONE NOT NULL,
    eindDatum TIMESTAMP WITH TIME ZONE,
    koppelingStatus text COLLATE pg_catalog."default" NOT NULL
);

CREATE INDEX koppelingen_bestekUuid_idx ON bestekkoppelingen (bestekUuid);
CREATE INDEX koppelingen_assetUuid_idx ON bestekkoppelingen (assetUuid);

ALTER TABLE IF EXISTS public.bestekkoppelingen
    ADD CONSTRAINT bestekkoppelingen_bestekken_fkey
    FOREIGN KEY (bestekUuid)
    REFERENCES public.bestekken (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.bestekkoppelingen
    ADD CONSTRAINT bestekkoppelingen_assets_fkey
    FOREIGN KEY (assetUuid)
    REFERENCES public.assets (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

-- Table: public.locatie

DROP TABLE IF EXISTS public.locatie CASCADE;
CREATE TABLE IF NOT EXISTS public.locatie
(
    assetUuid uuid NOT NULL,
    geometrie text COLLATE pg_catalog."default",
    omschrijving text COLLATE pg_catalog."default",
    bron text COLLATE pg_catalog."default",
    precisie text COLLATE pg_catalog."default",
    x decimal,
    y decimal,
    z decimal,
    ident8 text COLLATE pg_catalog."default",
    ident2 text COLLATE pg_catalog."default",
    referentiepaal_opschrift decimal,
    referentiepaal_afstand integer,
    straatnaam text COLLATE pg_catalog."default",
    gemeente text COLLATE pg_catalog."default",
    adres_straat text COLLATE pg_catalog."default",
    adres_nummer text COLLATE pg_catalog."default",
    adres_bus text COLLATE pg_catalog."default",
    adres_postcode text COLLATE pg_catalog."default",
    adres_gemeente text COLLATE pg_catalog."default",
    adres_provincie text COLLATE pg_catalog."default",
    geometry geometry
);

ALTER TABLE IF EXISTS public.locatie
    ADD CONSTRAINT assets_locatie_fkey
    FOREIGN KEY (assetUuid)
    REFERENCES public.assets (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

CREATE UNIQUE INDEX locatie_assetUuid
ON public.locatie (assetUuid);

ALTER TABLE locatie
ADD CONSTRAINT unique_locatie_assetUuid
UNIQUE USING INDEX locatie_assetUuid;

-- Table: public.geometrie

DROP TABLE IF EXISTS public.geometrie CASCADE;
CREATE TABLE IF NOT EXISTS public.geometrie
(
    assetUuid uuid NOT NULL,
    geo_niveau integer NOT NULL,
    ga_versie text COLLATE pg_catalog."default",
    nauwkeurigheid text COLLATE pg_catalog."default",
    bron text COLLATE pg_catalog."default",
    wkt_string text COLLATE pg_catalog."default",
    overerving_ids text COLLATE pg_catalog."default"
);

ALTER TABLE IF EXISTS public.geometrie
    ADD CONSTRAINT assets_geometrie_fkey
    FOREIGN KEY (assetUuid)
    REFERENCES public.assets (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

-- Table: public.elek_aansluitingen

DROP TABLE IF EXISTS public.elek_aansluitingen CASCADE;
CREATE TABLE IF NOT EXISTS public.elek_aansluitingen
(
    assetUuid uuid NOT NULL,
    EAN text COLLATE pg_catalog."default",
    aansluiting text COLLATE pg_catalog."default"
);

ALTER TABLE IF EXISTS public.elek_aansluitingen
    ADD CONSTRAINT assets_elek_aansluitingen_fkey
    FOREIGN KEY (assetUuid)
    REFERENCES public.assets (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


-- Table: public.vplan_koppelingen

DROP TABLE IF EXISTS public.vplan_koppelingen CASCADE;
CREATE TABLE IF NOT EXISTS public.vplan_koppelingen
(
    uuid uuid NOT NULL,
    assetUuid uuid NOT NULL,
    vplannummer text COLLATE pg_catalog."default",
    vplan uuid,
    inDienstDatum TIMESTAMP WITH TIME ZONE,
    uitDienstDatum TIMESTAMP WITH TIME ZONE,
    commentaar text COLLATE pg_catalog."default",
    PRIMARY KEY (uuid)
);

ALTER TABLE IF EXISTS public.vplan_koppelingen
    ADD CONSTRAINT assets_vplan_koppelingen_fkey
    FOREIGN KEY (assetUuid)
    REFERENCES public.assets (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

-- Table: public.toezichtgroepen

DROP TABLE IF EXISTS public.toezichtgroepen CASCADE;
CREATE TABLE IF NOT EXISTS public.toezichtgroepen
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default",
    typeGroep text COLLATE pg_catalog."default",
    referentie text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    CONSTRAINT toezichtgroepen_pkey PRIMARY KEY (uuid)
);

ALTER TABLE IF EXISTS public.assets
    ADD CONSTRAINT toezichtgroepen_assets_fkey
    FOREIGN KEY (toezichtgroep)
    REFERENCES public.toezichtgroepen (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


-- Table: public.identiteiten

DROP TABLE IF EXISTS public.identiteiten CASCADE;
CREATE TABLE IF NOT EXISTS public.identiteiten
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default",
    voornaam text COLLATE pg_catalog."default",
    gebruikersnaam text COLLATE pg_catalog."default",
    typeIdentiteit text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    systeem boolean NOT NULL,
    voId text COLLATE pg_catalog."default",
    bron text COLLATE pg_catalog."default",
    CONSTRAINT identiteiten_pkey PRIMARY KEY (uuid)
);

ALTER TABLE IF EXISTS public.assets
    ADD CONSTRAINT identiteiten_assets_fkey
    FOREIGN KEY (toezichter)
    REFERENCES public.identiteiten (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


-- Table: public.beheerders

DROP TABLE IF EXISTS public.beheerders CASCADE;
CREATE TABLE IF NOT EXISTS public.beheerders
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default",
    referentie text COLLATE pg_catalog."default",
    typeBeheerder text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    CONSTRAINT beheerders_pkey PRIMARY KEY (uuid)
);

ALTER TABLE IF EXISTS public.assets
    ADD CONSTRAINT beheerders_assets_fkey
    FOREIGN KEY (schadebeheerder)
    REFERENCES public.beheerders (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


-- Table: public.weglocaties

DROP TABLE IF EXISTS public.weglocaties CASCADE;
CREATE TABLE IF NOT EXISTS public.weglocaties
(
    assetUuid uuid NOT NULL,
    geometrie text COLLATE pg_catalog."default",
    score text COLLATE pg_catalog."default",
    bron text COLLATE pg_catalog."default",
    CONSTRAINT weglocaties_pkey PRIMARY KEY (assetUuid)
);

ALTER TABLE IF EXISTS public.weglocaties
    ADD CONSTRAINT assets_weglocaties_fkey
    FOREIGN KEY (assetUuid)
    REFERENCES public.assets (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


 -- Table: public.weglocatie_wegsegmenten

DROP TABLE IF EXISTS public.weglocatie_wegsegmenten CASCADE;
CREATE TABLE IF NOT EXISTS public.weglocatie_wegsegmenten
(
    assetUuid uuid NOT NULL,
    oidn integer NOT NULL
);

CREATE INDEX weglocatie_wegsegmenten_assetUuid_idx ON weglocatie_wegsegmenten (assetUuid);

ALTER TABLE IF EXISTS public.weglocatie_wegsegmenten
    ADD CONSTRAINT assets_weglocatie_wegsegmenten_fkey
    FOREIGN KEY (assetUuid)
    REFERENCES public.assets (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


  -- Table: public.weglocatie_aanduidingen

DROP TABLE IF EXISTS public.weglocatie_aanduidingen CASCADE;
CREATE TABLE IF NOT EXISTS public.weglocatie_aanduidingen
(
    assetUuid uuid NOT NULL,
    wegnummer text COLLATE pg_catalog."default",
    van_wegnummer text COLLATE pg_catalog."default",
    van_ref_wegnummer text COLLATE pg_catalog."default",
    van_ref_opschrift text COLLATE pg_catalog."default",
    van_afstand integer,
    tot_wegnummer text COLLATE pg_catalog."default",
    tot_ref_wegnummer text COLLATE pg_catalog."default",
    tot_ref_opschrift text COLLATE pg_catalog."default",
    tot_afstand integer
);

CREATE INDEX weglocatie_aanduidingen_assetUuid_idx ON weglocatie_aanduidingen (assetUuid);

ALTER TABLE IF EXISTS public.weglocatie_aanduidingen
    ADD CONSTRAINT assets_weglocatie_aanduidingen_fkey
    FOREIGN KEY (assetUuid)
    REFERENCES public.assets (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


-- Table: public.betrokkeneRelaties

DROP TABLE IF EXISTS public.betrokkeneRelaties CASCADE;
CREATE TABLE IF NOT EXISTS public.betrokkeneRelaties
(
    uuid uuid NOT NULL,
    doelUuid uuid NOT NULL,
    bronUuid uuid NOT NULL,
    bronAgentUuid uuid,
    bronAssetUuid uuid,
    rol text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    contact_info json,
    startDatum TIMESTAMP WITH TIME ZONE,
    eindDatum TIMESTAMP WITH TIME ZONE,
    CONSTRAINT betrokkeneRelaties_pkey PRIMARY KEY (uuid)
);

CREATE INDEX betrokkeneRelaties_doelUuid_idx ON betrokkeneRelaties (doelUuid);
CREATE INDEX betrokkeneRelaties_bronUuid_idx ON betrokkeneRelaties (bronUuid);

ALTER TABLE IF EXISTS public.betrokkeneRelaties
    ADD CONSTRAINT betrokkeneRelaties_agents_fkey
    FOREIGN KEY (doelUuid)
    REFERENCES public.agents (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.betrokkeneRelaties
    ADD CONSTRAINT betrokkeneRelaties_bron_agents_fkey
    FOREIGN KEY (bronAgentUuid)
    REFERENCES public.agents (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.betrokkeneRelaties
    ADD CONSTRAINT betrokkeneRelaties_bron_assets_fkey
    FOREIGN KEY (bronAssetUuid)
    REFERENCES public.assets (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

-- Table: public.relatietypes

DROP TABLE IF EXISTS public.relatietypes CASCADE;
CREATE TABLE IF NOT EXISTS public.relatietypes
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default",
    label text COLLATE pg_catalog."default",
    definitie text COLLATE pg_catalog."default",
    uri text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    gericht boolean NOT NULL,
    CONSTRAINT relatietypes_pkey PRIMARY KEY (uuid)
);

-- Table: public.assetRelaties

DROP TABLE IF EXISTS public.assetRelaties CASCADE;
CREATE TABLE IF NOT EXISTS public.assetRelaties
(
    uuid uuid NOT NULL,
    bronUuid uuid NOT NULL,
    doelUuid uuid NOT NULL,
    relatietype uuid NOT NULL,
    attributen json,
    actief boolean NOT NULL,
    CONSTRAINT assetRelaties_pkey PRIMARY KEY (uuid)
);

CREATE INDEX assetRelaties_bronUuid_idx ON assetRelaties (bronUuid);
CREATE INDEX assetRelaties_doelUuid_idx ON assetRelaties (doelUuid);
CREATE INDEX assetRelaties_relatietype_idx ON assetRelaties (relatietype);

ALTER TABLE IF EXISTS public.assetRelaties
    ADD CONSTRAINT assetRelaties_bronUuid_fkey
    FOREIGN KEY (bronUuid)
    REFERENCES public.assets (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.assetRelaties
    ADD CONSTRAINT assetRelaties_doelUuid_fkey
    FOREIGN KEY (doelUuid)
    REFERENCES public.assets (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE IF EXISTS public.assetRelaties
    ADD CONSTRAINT assetRelaties_relatietype_fkey
    FOREIGN KEY (relatietype)
    REFERENCES public.relatietypes (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

CREATE VIEW aantallen AS
SELECT 'agents' AS naam, count(*) AS aantal FROM agents
UNION ALL
SELECT 'assets', count(*) FROM assets
UNION ALL
SELECT 'assetrelaties', count(*) FROM assetrelaties
UNION ALL
SELECT 'assettypes', count(*) FROM assettypes
UNION ALL
SELECT 'attributen', count(*) FROM attributen
UNION ALL
SELECT 'attribuutWaarden', count(*) FROM attribuutWaarden
UNION ALL
SELECT 'beheerders', count(*) FROM beheerders
UNION ALL
SELECT 'bestekken', count(*) FROM bestekken
UNION ALL
SELECT 'bestekkoppelingen', count(*) FROM bestekkoppelingen
UNION ALL
SELECT 'betrokkenerelaties', count(*) FROM betrokkenerelaties
UNION ALL
SELECT 'elek_aansluitingen', count(*) FROM elek_aansluitingen
UNION ALL
SELECT 'identiteiten', count(*) FROM identiteiten
UNION ALL
SELECT 'relatietypes', count(*) FROM relatietypes
UNION ALL
SELECT 'toezichtgroepen', count(*) FROM toezichtgroepen
UNION ALL
SELECT 'vplan_koppelingen', count(*) FROM vplan_koppelingen;