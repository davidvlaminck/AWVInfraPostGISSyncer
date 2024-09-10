CREATE TABLE public.gemeente (
	gemeente varchar(64) NULL,
	niscode varchar(5) NULL,
	provincie varchar NULL,
	geom public.geometry(geometry, 31370) NULL,
	CONSTRAINT gemeente_gemeente_key UNIQUE (gemeente),
	CONSTRAINT gemeente_niscode_key UNIQUE (niscode)
);
CREATE INDEX gemeente_geom_sidx ON public.gemeente USING gist (geom);
CREATE INDEX gemeente_provincie_key ON public.gemeente USING btree (provincie);