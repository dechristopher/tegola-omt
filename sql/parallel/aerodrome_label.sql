DO $$ BEGIN RAISE NOTICE 'Processing layer aerodrome_label'; END$$;

-- Layer aerodrome_label - ./update_aerodrome_label_point.sql

DROP TRIGGER IF EXISTS trigger_flag ON osm_aerodrome_label_point;
DROP TRIGGER IF EXISTS trigger_store ON osm_aerodrome_label_point;
DROP TRIGGER IF EXISTS trigger_refresh ON aerodrome_label.updates;

CREATE SCHEMA IF NOT EXISTS aerodrome_label;

CREATE TABLE IF NOT EXISTS aerodrome_label.osm_ids
(
    osm_id bigint
);

-- etldoc: osm_aerodrome_label_point -> osm_aerodrome_label_point
CREATE OR REPLACE FUNCTION update_aerodrome_label_point(full_update boolean) RETURNS void AS
$$
    UPDATE osm_aerodrome_label_point
    SET geometry = ST_Centroid(geometry)
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM aerodrome_label.osm_ids))
        AND ST_GeometryType(geometry) <> 'ST_Point';

    UPDATE osm_aerodrome_label_point
    SET tags = update_tags(tags, geometry)
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM aerodrome_label.osm_ids))
        AND COALESCE(tags->'name:latin', tags->'name:nonlatin', tags->'name_int') IS NULL
        AND tags != update_tags(tags, geometry);
$$ LANGUAGE SQL;

SELECT update_aerodrome_label_point(true);

-- Handle updates

CREATE OR REPLACE FUNCTION aerodrome_label.store() RETURNS trigger AS
$$
BEGIN
    IF (tg_op = 'DELETE') THEN
        INSERT INTO aerodrome_label.osm_ids VALUES (OLD.osm_id);
    ELSE
        INSERT INTO aerodrome_label.osm_ids VALUES (NEW.osm_id);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS aerodrome_label.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION aerodrome_label.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO aerodrome_label.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION aerodrome_label.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh aerodrome_label';
    PERFORM update_aerodrome_label_point(false);
    -- noinspection SqlWithoutWhere
    DELETE FROM aerodrome_label.osm_ids;
    -- noinspection SqlWithoutWhere
    DELETE FROM aerodrome_label.updates;

    RAISE LOG 'Refresh aerodrome_label done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_store
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_aerodrome_label_point
    FOR EACH ROW
EXECUTE PROCEDURE aerodrome_label.store();

CREATE TRIGGER trigger_flag
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_aerodrome_label_point
    FOR EACH STATEMENT
EXECUTE PROCEDURE aerodrome_label.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON aerodrome_label.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE aerodrome_label.refresh();

-- Layer aerodrome_label - ./aerodrome_label.sql


-- etldoc: layer_aerodrome_label[shape=record fillcolor=lightpink, style="rounded,filled", label="layer_aerodrome_label | <z10_> z10+" ] ;

CREATE OR REPLACE FUNCTION layer_aerodrome_label(bbox geometry,
                                                 zoom_level integer)
    RETURNS TABLE
            (
                osm_id   bigint,
                geometry geometry,
                name     text,
                name_en  text,
                name_de  text,
                tags     hstore,
                class    text,
                iata     text,
                icao     text,
                ele      int,
                ele_ft   int
            )
AS
$$
SELECT
    -- etldoc: osm_aerodrome_label_point -> layer_aerodrome_label:z10_
    osm_id,
    geometry,
    name,
    COALESCE(NULLIF(name_en, ''), name) AS name_en,
    COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
    tags,
    CASE
        WHEN "aerodrome" = 'international'
            OR "aerodrome_type" = 'international'
            THEN 'international'
        WHEN "aerodrome" = 'public'
            OR "aerodrome_type" = 'civil'
            OR "aerodrome_type" LIKE '%public%'
            THEN 'public'
        WHEN "aerodrome" = 'regional'
            OR "aerodrome_type" = 'regional'
            THEN 'regional'
        WHEN "aerodrome" = 'military'
            OR "aerodrome_type" LIKE '%military%'
            OR "military" = 'airfield'
            THEN 'military'
        WHEN "aerodrome" = 'private'
            OR "aerodrome_type" = 'private'
            THEN 'private'
        ELSE 'other'
        END AS class,
    NULLIF(iata, '') AS iata,
    NULLIF(icao, '') AS icao,
    substring(ele FROM E'^(-?\\d+)(\\D|$)')::int AS ele,
    round(substring(ele FROM E'^(-?\\d+)(\\D|$)')::int * 3.2808399)::int AS ele_ft
FROM osm_aerodrome_label_point
WHERE geometry && bbox
  AND zoom_level >= 10;
$$ LANGUAGE SQL STABLE
                -- STRICT
                PARALLEL SAFE;

DO $$ BEGIN RAISE NOTICE 'Finished layer aerodrome_label'; END$$;
