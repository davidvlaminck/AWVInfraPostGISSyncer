-- Drop table
-- DROP TABLE regex;

CREATE TABLE if not exists regex (
	id serial4 NOT NULL,
	uri varchar NOT NULL, -- typeURI of asset
	pattern varchar NULL, -- regex pattern
	description varchar NULL,
	count_null numeric NULL, -- Count the number of records that have NULL values
	count_valid numeric NULL, -- count the number of valid names
	count_invalid numeric NULL, -- count the number of invalid names
	count numeric NULL, -- Som van de aantal van NULL, valid en invallid
	updated_at timestamp DEFAULT CURRENT_DATE NULL, -- timestamp at which the calculations are updated
	validated bool DEFAULT false NULL, -- De regex-expressie is geverifieerd door de Business
	CONSTRAINT regex_pk PRIMARY KEY (id),
	CONSTRAINT regex_unique UNIQUE (uri)
);
COMMENT ON TABLE regex IS 'Regular expression syntax for asset name validation';

-- Column comments
COMMENT ON COLUMN regex.uri IS 'typeURI of asset';
COMMENT ON COLUMN regex.pattern IS 'regex pattern';
COMMENT ON COLUMN regex.count_null IS 'Count the number of records that have NULL values';
COMMENT ON COLUMN regex.count_valid IS 'count the number of valid names';
COMMENT ON COLUMN regex.count_invalid IS 'count the number of invalid names';
COMMENT ON COLUMN regex.count IS 'Som van de aantal van NULL, valid en invallid';
COMMENT ON COLUMN regex.updated_at IS 'timestamp at which the calculations are updated';
COMMENT ON COLUMN regex.validated IS 'De regex-expressie is geverifieerd door de Business';

-- DROP FUNCTION update_count_column();
CREATE OR REPLACE FUNCTION update_count_column()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.count = coalesce(NEW.count_null, 0) + coalesce(NEW.count_valid, 0) + coalesce(NEW.count_invalid, 0);
    RETURN NEW;
END;
$function$
;

-- DROP FUNCTION update_updated_at_column();
CREATE OR REPLACE FUNCTION update_updated_at_column()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$function$
;



-- Table Triggers
create trigger set_count before
insert
    or
update
    on
    regex for each row execute function update_count_column();
create trigger set_updated_at before
insert
    or
update
    on
    regex for each row execute function update_updated_at_column();