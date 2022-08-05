DROP TABLE IF EXISTS public.agents;
CREATE TABLE public.agents
(
    uuid uuid NOT NULL,
    naam text,
    void uuid,
    actief boolean NOT NULL,
    contact_info json,
    PRIMARY KEY (uuid)
);

DROP TABLE IF EXISTS public.params;
CREATE TABLE IF NOT EXISTS public.params
(
    page integer,
    event_uuid text,
    pagesize integer,
    freshstart boolean,
    otltype integer,
    pagingcursor text COLLATE pg_catalog."default"
);

INSERT INTO public.params(page, event_uuid, pagesize, freshstart, otltype, pagingcursor)
VALUES (-1, '', 100, TRUE, -1, '');

DROP TABLE IF EXISTS public.assets;
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

DROP TABLE IF EXISTS public.assettypes;
CREATE TABLE IF NOT EXISTS public.assettypes
(
    uuid uuid NOT NULL,
    naam text COLLATE pg_catalog."default" NOT NULL,
    label text COLLATE pg_catalog."default" NOT NULL,
    uri text COLLATE pg_catalog."default" NOT NULL,
    definitie text COLLATE pg_catalog."default",
    actief boolean NOT NULL,
    bestek boolean,
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

DROP TABLE IF EXISTS public.bestekken;
CREATE TABLE IF NOT EXISTS public.bestekken
(
    uuid uuid NOT NULL,
    eDeltaDossiernummer text COLLATE pg_catalog."default" NOT NULL,
    eDeltaBesteknummer text COLLATE pg_catalog."default" NOT NULL,
    aannemerNaam text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT bestekken_pkey PRIMARY KEY (uuid)
);

DROP TABLE IF EXISTS public.bestekkoppelingen;
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



-- psycopg2.errors.ForeignKeyViolation: insert or update on table "assets" violates foreign key constraint "assets_assettype_fkey"
-- DETAIL:  Key (assettype)=(00000453-56ce-4f8b-af44-960df526cb30) is not present in table "assettypes".