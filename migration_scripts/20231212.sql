

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
