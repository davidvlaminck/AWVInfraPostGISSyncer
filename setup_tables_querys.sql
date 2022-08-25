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
    page integer,
    event_uuid text,
    pagesize integer,
    fresh_start boolean,
    sync_step integer,
    pagingcursor text COLLATE pg_catalog."default",
    last_update_utc TIMESTAMP WITH TIME ZONE
);

INSERT INTO public.params(page, event_uuid, pagesize, fresh_start, sync_step, pagingcursor)
VALUES (-1, '', 100, TRUE, -1, '');

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
    elekAansluiting text COLLATE pg_catalog."default",
    EAN text COLLATE pg_catalog."default",
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
    CONSTRAINT assettypes_pkey PRIMARY KEY (uuid)
);

ALTER TABLE IF EXISTS public.assets
    ADD CONSTRAINT assets_assettype_fkey
    FOREIGN KEY (assettype)
    REFERENCES public.assettypes (uuid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

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
    adres_provincie text COLLATE pg_catalog."default"
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
    niveau integer NOT NULL,
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

-- PostGIS

CREATE EXTENSION postgis;


-- psycopg2.errors.ForeignKeyViolation: insert or update on table "assets" violates foreign key constraint "assets_assettype_fkey"
-- DETAIL:  Key (assettype)=(00000453-56ce-4f8b-af44-960df526cb30) is not present in table "assettypes".