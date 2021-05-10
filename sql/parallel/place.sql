DO $$ BEGIN RAISE NOTICE 'Processing layer place'; END$$;

-- Layer place - ./types.sql

DO
$$
    BEGIN
        IF NOT EXISTS(SELECT 1 FROM pg_type WHERE typname = 'city_place') THEN
            CREATE TYPE city_place AS enum ('city', 'town', 'village', 'hamlet', 'suburb', 'quarter', 'neighbourhood', 'isolated_dwelling');
        END IF;
    END
$$;

ALTER TABLE osm_city_point
    ALTER COLUMN place TYPE city_place USING place::city_place;

-- Layer place - ./capital.sql

CREATE OR REPLACE FUNCTION normalize_capital_level(capital text)
    RETURNS int AS
$$
SELECT CASE
           WHEN capital IN ('yes', '2') THEN 2
           WHEN capital = '4' THEN 4
           END;
$$ LANGUAGE SQL IMMUTABLE
                STRICT
                PARALLEL SAFE;

-- Layer place - ./city.sql

-- etldoc: layer_city[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="layer_city | <z2_14> z2-z14+" ] ;

-- etldoc: osm_city_point -> layer_city:z2_14
CREATE OR REPLACE FUNCTION layer_city(bbox geometry, zoom_level int, pixel_width numeric)
    RETURNS TABLE
            (
                osm_id   bigint,
                geometry geometry,
                name     text,
                name_en  text,
                name_de  text,
                tags     hstore,
                place    city_place,
                "rank"   int,
                capital  int
            )
AS
$$
SELECT *
FROM (
         SELECT osm_id,
                geometry,
                name,
                COALESCE(NULLIF(name_en, ''), name) AS name_en,
                COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
                tags,
                place,
                "rank",
                normalize_capital_level(capital) AS capital
         FROM osm_city_point
         WHERE geometry && bbox
           AND ((zoom_level = 2 AND "rank" = 1)
             OR (zoom_level BETWEEN 3 AND 7 AND "rank" <= zoom_level + 1)
             )
         UNION ALL
         SELECT osm_id,
                geometry,
                name,
                COALESCE(NULLIF(name_en, ''), name) AS name_en,
                COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
                tags,
                place,
                COALESCE("rank", gridrank + 10),
                normalize_capital_level(capital) AS capital
         FROM (
                  SELECT osm_id,
                         geometry,
                         name,
                         COALESCE(NULLIF(name_en, ''), name) AS name_en,
                         COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
                         tags,
                         place,
                         "rank",
                         capital,
                         row_number() OVER (
                             PARTITION BY LabelGrid(geometry, 128 * pixel_width)
                             ORDER BY "rank" ASC NULLS LAST,
                                 place ASC NULLS LAST,
                                 population DESC NULLS LAST,
                                 length(name) ASC
                             )::int AS gridrank
                  FROM osm_city_point
                  WHERE geometry && bbox
                    AND ((zoom_level = 7 AND place <= 'town'::city_place
                      OR (zoom_level BETWEEN 8 AND 10 AND place <= 'village'::city_place)
                      OR (zoom_level BETWEEN 11 AND 13 AND place <= 'suburb'::city_place)
                      OR (zoom_level >= 14)
                      ))
              ) AS ranked_places
         WHERE (zoom_level BETWEEN 7 AND 8 AND (gridrank <= 4 OR "rank" IS NOT NULL))
            OR (zoom_level = 9 AND (gridrank <= 8 OR "rank" IS NOT NULL))
            OR (zoom_level = 10 AND (gridrank <= 12 OR "rank" IS NOT NULL))
            OR (zoom_level BETWEEN 11 AND 12 AND (gridrank <= 14 OR "rank" IS NOT NULL))
            OR (zoom_level >= 13)
     ) AS city_all;
$$ LANGUAGE SQL STABLE
                -- STRICT
                PARALLEL SAFE;

-- Layer place - ./island_rank.sql

CREATE OR REPLACE FUNCTION island_rank(area real) RETURNS int AS
$$
SELECT CASE
           WHEN area < 10000000 THEN 6
           WHEN area BETWEEN 1000000 AND 15000000 THEN 5
           WHEN area BETWEEN 15000000 AND 40000000 THEN 4
           WHEN area > 40000000 THEN 3
           ELSE 7
           END;
$$ LANGUAGE SQL IMMUTABLE
                STRICT
                PARALLEL SAFE;

-- Layer place - ./update_continent_point.sql

DROP TRIGGER IF EXISTS trigger_flag ON osm_continent_point;
DROP TRIGGER IF EXISTS trigger_store ON osm_continent_point;
DROP TRIGGER IF EXISTS trigger_refresh ON place_continent_point.updates;

CREATE SCHEMA IF NOT EXISTS place_continent_point;

CREATE TABLE IF NOT EXISTS place_continent_point.osm_ids
(
    osm_id bigint
);

-- etldoc:  osm_continent_point ->  osm_continent_point
CREATE OR REPLACE FUNCTION update_osm_continent_point(full_update boolean) RETURNS void AS
$$
    UPDATE osm_continent_point
    SET tags = update_tags(tags, geometry)
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM place_continent_point.osm_ids))
      AND COALESCE(tags->'name:latin', tags->'name:nonlatin', tags->'name_int') IS NULL
      AND tags != update_tags(tags, geometry);
$$ LANGUAGE SQL;

SELECT update_osm_continent_point(true);

-- Handle updates

CREATE OR REPLACE FUNCTION place_continent_point.store() RETURNS trigger AS
$$
BEGIN
    IF (tg_op = 'DELETE') THEN
        INSERT INTO place_continent_point.osm_ids VALUES (OLD.osm_id);
    ELSE
        INSERT INTO place_continent_point.osm_ids VALUES (NEW.osm_id);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS place_continent_point.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION place_continent_point.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO place_continent_point.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION place_continent_point.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh place_continent_point';
    PERFORM update_osm_continent_point(false);
    -- noinspection SqlWithoutWhere
    DELETE FROM place_continent_point.osm_ids;
    -- noinspection SqlWithoutWhere
    DELETE FROM place_continent_point.updates;

    RAISE LOG 'Refresh place_continent_point done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_store
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_continent_point
    FOR EACH ROW
EXECUTE PROCEDURE place_continent_point.store();

CREATE TRIGGER trigger_flag
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_continent_point
    FOR EACH STATEMENT
EXECUTE PROCEDURE place_continent_point.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON place_continent_point.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE place_continent_point.refresh();

-- Layer place - ./update_country_point.sql

DROP TRIGGER IF EXISTS trigger_flag ON osm_country_point;
DROP TRIGGER IF EXISTS trigger_store ON osm_country_point;
DROP TRIGGER IF EXISTS trigger_refresh ON place_country.updates;

CREATE SCHEMA IF NOT EXISTS place_country;

CREATE TABLE IF NOT EXISTS place_country.osm_ids
(
    osm_id bigint
);

-- etldoc: ne_10m_admin_0_countries   -> osm_country_point
-- etldoc: osm_country_point          -> osm_country_point

CREATE OR REPLACE FUNCTION update_osm_country_point(full_update boolean) RETURNS void AS
$$
    UPDATE osm_country_point AS osm
    SET "rank"            = 7,
        iso3166_1_alpha_2 = COALESCE(
                NULLIF(osm.country_code_iso3166_1_alpha_2, ''),
                NULLIF(osm.iso3166_1_alpha_2, ''),
                NULLIF(osm.iso3166_1, '')
            )
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM place_country.osm_ids))
      AND rank IS NULL;

    WITH important_country_point AS (
        SELECT osm.geometry,
               osm.osm_id,
               osm.name,
               COALESCE(NULLIF(osm.name_en, ''), ne.name) AS name_en,
               ne.scalerank,
               ne.labelrank
        FROM ne_10m_admin_0_countries AS ne,
             osm_country_point AS osm
        WHERE
          -- We match only countries with ISO codes to eliminate disputed countries
            iso3166_1_alpha_2 IS NOT NULL
          -- that lies inside polygon of sovereign country
          AND ST_Within(osm.geometry, ne.geometry)
    )
    UPDATE osm_country_point AS osm
        -- Normalize both scalerank and labelrank into a ranking system from 1 to 6
        -- where the ranks are still distributed uniform enough across all countries
    SET "rank" = LEAST(6, CEILING((scalerank + labelrank) / 2.0))
    FROM important_country_point AS ne
    WHERE (full_update OR osm.osm_id IN (SELECT osm_id FROM place_country.osm_ids))
      AND rank = 7
      AND osm.osm_id = ne.osm_id;

    -- Repeat the step for archipelago countries like Philippines or Indonesia
    -- whose label point is not within country's polygon
    WITH important_country_point AS (
        SELECT osm.osm_id,
--       osm.name,
               ne.scalerank,
               ne.labelrank,
--       ST_Distance(osm.geometry, ne.geometry) AS distance,
               ROW_NUMBER()
               OVER (
                   PARTITION BY osm.osm_id
                   ORDER BY
                       ST_Distance(osm.geometry, ne.geometry)
                   ) AS rk
        FROM osm_country_point osm,
             ne_10m_admin_0_countries AS ne
        WHERE iso3166_1_alpha_2 IS NOT NULL
          AND NOT (osm."rank" BETWEEN 1 AND 6)
    )
    UPDATE osm_country_point AS osm
        -- Normalize both scalerank and labelrank into a ranking system from 1 to 6
        -- where the ranks are still distributed uniform enough across all countries
    SET "rank" = LEAST(6, CEILING((ne.scalerank + ne.labelrank) / 2.0))
    FROM important_country_point AS ne
    WHERE (full_update OR osm.osm_id IN (SELECT osm_id FROM place_country.osm_ids))
      AND rank = 7
      AND osm.osm_id = ne.osm_id
      AND ne.rk = 1;

    UPDATE osm_country_point AS osm
    SET "rank" = 6
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM place_country.osm_ids))
      AND "rank" = 7;

    -- TODO: This shouldn't be necessary? The rank function makes something wrong...
    UPDATE osm_country_point AS osm
    SET "rank" = 1
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM place_country.osm_ids))
      AND "rank" = 0;

    UPDATE osm_country_point
    SET tags = update_tags(tags, geometry)
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM place_country.osm_ids))
      AND COALESCE(tags->'name:latin', tags->'name:nonlatin', tags->'name_int') IS NULL
      AND tags != update_tags(tags, geometry);

$$ LANGUAGE SQL;

SELECT update_osm_country_point(true);

CREATE INDEX IF NOT EXISTS osm_country_point_rank_idx ON osm_country_point ("rank");

-- Handle updates

CREATE OR REPLACE FUNCTION place_country.store() RETURNS trigger AS
$$
BEGIN
    IF (tg_op = 'DELETE') THEN
        INSERT INTO place_country.osm_ids VALUES (OLD.osm_id);
    ELSE
        INSERT INTO place_country.osm_ids VALUES (NEW.osm_id);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS place_country.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION place_country.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO place_country.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION place_country.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh place_country rank';
    PERFORM update_osm_country_point(false);
    -- noinspection SqlWithoutWhere
    DELETE FROM place_country.osm_ids;
    -- noinspection SqlWithoutWhere
    DELETE FROM place_country.updates;

    RAISE LOG 'Refresh place_country done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_store
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_country_point
    FOR EACH ROW
EXECUTE PROCEDURE place_country.store();

CREATE TRIGGER trigger_flag
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_country_point
    FOR EACH STATEMENT
EXECUTE PROCEDURE place_country.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON place_country.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE place_country.refresh();

-- Layer place - ./update_island_polygon.sql

DROP TRIGGER IF EXISTS trigger_flag ON osm_island_polygon;
DROP TRIGGER IF EXISTS trigger_store ON osm_island_polygon;
DROP TRIGGER IF EXISTS trigger_refresh ON place_island_polygon.updates;

CREATE SCHEMA IF NOT EXISTS place_island_polygon;

CREATE TABLE IF NOT EXISTS place_island_polygon.osm_ids
(
    osm_id bigint
);

-- etldoc:  osm_island_polygon ->  osm_island_polygon
CREATE OR REPLACE FUNCTION update_osm_island_polygon(full_update boolean) RETURNS void AS
$$
    UPDATE osm_island_polygon
    SET geometry = ST_PointOnSurface(geometry)
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM place_island_polygon.osm_ids))
      AND ST_GeometryType(geometry) <> 'ST_Point'
      AND ST_IsValid(geometry);

    UPDATE osm_island_polygon
    SET tags = update_tags(tags, geometry)
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM place_island_polygon.osm_ids))
      AND COALESCE(tags->'name:latin', tags->'name:nonlatin', tags->'name_int') IS NULL
      AND tags != update_tags(tags, geometry);

$$ LANGUAGE SQL;

SELECT update_osm_island_polygon(true);

-- Handle updates

CREATE OR REPLACE FUNCTION place_island_polygon.store() RETURNS trigger AS
$$
BEGIN
    IF (tg_op = 'DELETE') THEN
        INSERT INTO place_island_polygon.osm_ids VALUES (OLD.osm_id);
    ELSE
        INSERT INTO place_island_polygon.osm_ids VALUES (NEW.osm_id);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS place_island_polygon.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION place_island_polygon.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO place_island_polygon.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION place_island_polygon.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh place_island_polygon';
    PERFORM update_osm_island_polygon(false);
    -- noinspection SqlWithoutWhere
    DELETE FROM place_island_polygon.osm_ids;
    -- noinspection SqlWithoutWhere
    DELETE FROM place_island_polygon.updates;

    RAISE LOG 'Refresh place_island_polygon done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_store
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_island_polygon
    FOR EACH ROW
EXECUTE PROCEDURE place_island_polygon.store();

CREATE TRIGGER trigger_flag
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_island_polygon
    FOR EACH STATEMENT
EXECUTE PROCEDURE place_island_polygon.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON place_island_polygon.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE place_island_polygon.refresh();

-- Layer place - ./update_island_point.sql

DROP TRIGGER IF EXISTS trigger_flag ON osm_island_point;
DROP TRIGGER IF EXISTS trigger_store ON osm_island_point;
DROP TRIGGER IF EXISTS trigger_refresh ON place_island_point.updates;

CREATE SCHEMA IF NOT EXISTS place_island_point;

CREATE TABLE IF NOT EXISTS place_island_point.osm_ids
(
    osm_id bigint
);

-- etldoc:  osm_island_point ->  osm_island_point
CREATE OR REPLACE FUNCTION update_osm_island_point(full_update boolean) RETURNS void AS
$$
    UPDATE osm_island_point
    SET tags = update_tags(tags, geometry)
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM place_island_point.osm_ids))
      AND COALESCE(tags->'name:latin', tags->'name:nonlatin', tags->'name_int') IS NULL
      AND tags != update_tags(tags, geometry);
$$ LANGUAGE SQL;

SELECT update_osm_island_point(true);

-- Handle updates

CREATE OR REPLACE FUNCTION place_island_point.store() RETURNS trigger AS
$$
BEGIN
    IF (tg_op = 'DELETE') THEN
        INSERT INTO place_island_point.osm_ids VALUES (OLD.osm_id);
    ELSE
        INSERT INTO place_island_point.osm_ids VALUES (NEW.osm_id);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS place_island_point.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION place_island_point.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO place_island_point.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION place_island_point.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh place_island_point';
    PERFORM update_osm_island_point(false);
    -- noinspection SqlWithoutWhere
    DELETE FROM place_island_point.osm_ids;
    -- noinspection SqlWithoutWhere
    DELETE FROM place_island_point.updates;

    RAISE LOG 'Refresh place_island_point done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_store
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_island_point
    FOR EACH ROW
EXECUTE PROCEDURE place_island_point.store();

CREATE TRIGGER trigger_flag
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_island_point
    FOR EACH STATEMENT
EXECUTE PROCEDURE place_island_point.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON place_island_point.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE place_island_point.refresh();

-- Layer place - ./update_state_point.sql

DROP TRIGGER IF EXISTS trigger_flag ON osm_state_point;
DROP TRIGGER IF EXISTS trigger_store ON osm_state_point;
DROP TRIGGER IF EXISTS trigger_refresh ON place_state.updates;

CREATE SCHEMA IF NOT EXISTS place_state;

CREATE TABLE IF NOT EXISTS place_state.osm_ids
(
    osm_id bigint
);

-- etldoc: ne_10m_admin_1_states_provinces   -> osm_state_point
-- etldoc: osm_state_point                       -> osm_state_point

CREATE OR REPLACE FUNCTION update_osm_state_point(full_update boolean) RETURNS void AS
$$
    WITH important_state_point AS (
        SELECT osm.geometry,
               osm.osm_id,
               osm.name,
               COALESCE(NULLIF(osm.name_en, ''), ne.name) AS name_en,
               ne.scalerank,
               ne.labelrank,
               ne.datarank
        FROM ne_10m_admin_1_states_provinces AS ne,
             osm_state_point AS osm
        WHERE
          -- We only match whether the point is within the Natural Earth polygon
          -- because name matching is difficult
            ST_Within(osm.geometry, ne.geometry)
          -- We leave out leess important states
          AND ne.scalerank <= 3
          AND ne.labelrank <= 2
    )
    UPDATE osm_state_point AS osm
        -- Normalize both scalerank and labelrank into a ranking system from 1 to 6.
    SET "rank" = LEAST(6, CEILING((scalerank + labelrank + datarank) / 3.0))
    FROM important_state_point AS ne
    WHERE (full_update OR osm.osm_id IN (SELECT osm_id FROM place_state.osm_ids))
      AND rank IS NULL
      AND osm.osm_id = ne.osm_id;

    -- TODO: This shouldn't be necessary? The rank function makes something wrong...
    UPDATE osm_state_point AS osm
    SET "rank" = 1
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM place_state.osm_ids))
      AND "rank" = 0;

    DELETE FROM osm_state_point
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM place_state.osm_ids))
      AND "rank" IS NULL;

    UPDATE osm_state_point
    SET tags = update_tags(tags, geometry)
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM place_state.osm_ids))
      AND COALESCE(tags->'name:latin', tags->'name:nonlatin', tags->'name_int') IS NULL
      AND tags != update_tags(tags, geometry);

$$ LANGUAGE SQL;

SELECT update_osm_state_point(true);

CREATE INDEX IF NOT EXISTS osm_state_point_rank_idx ON osm_state_point ("rank");

-- Handle updates

CREATE OR REPLACE FUNCTION place_state.store() RETURNS trigger AS
$$
BEGIN
    IF (tg_op = 'DELETE') THEN
        INSERT INTO place_state.osm_ids VALUES (OLD.osm_id);
    ELSE
        INSERT INTO place_state.osm_ids VALUES (NEW.osm_id);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS place_state.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION place_state.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO place_state.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION place_state.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh place_state rank';
    PERFORM update_osm_state_point(false);
    -- noinspection SqlWithoutWhere
    DELETE FROM place_state.osm_ids;
    -- noinspection SqlWithoutWhere
    DELETE FROM place_state.updates;

    RAISE LOG 'Refresh place_state done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_store
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_state_point
    FOR EACH ROW
EXECUTE PROCEDURE place_state.store();

CREATE TRIGGER trigger_flag
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_state_point
    FOR EACH STATEMENT
EXECUTE PROCEDURE place_state.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON place_state.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE place_state.refresh();

-- Layer place - ./update_city_point.sql

DROP TRIGGER IF EXISTS trigger_flag ON osm_city_point;
DROP TRIGGER IF EXISTS trigger_store ON osm_city_point;
DROP TRIGGER IF EXISTS trigger_refresh ON place_city.updates;

CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE SCHEMA IF NOT EXISTS place_city;

CREATE TABLE IF NOT EXISTS place_city.osm_ids
(
    osm_id bigint
);

CREATE OR REPLACE FUNCTION update_osm_city_point(full_update boolean) RETURNS void AS
$$
    -- etldoc: ne_10m_populated_places -> osm_city_point
    -- etldoc: osm_city_point          -> osm_city_point

    WITH important_city_point AS (
        SELECT osm.osm_id, ne.scalerank
        FROM osm_city_point AS osm
             -- Clear OSM key:rank ( https://github.com/openmaptiles/openmaptiles/issues/108 )
             LEFT JOIN ne_10m_populated_places AS ne ON
            (
                (osm.tags ? 'wikidata' AND osm.tags->'wikidata' = ne.wikidataid) OR
                lower(osm.name) IN (lower(ne.name), lower(ne.namealt), lower(ne.meganame), lower(ne.gn_ascii), lower(ne.nameascii)) OR
                lower(osm.name_en) IN (lower(ne.name), lower(ne.namealt), lower(ne.meganame), lower(ne.gn_ascii), lower(ne.nameascii)) OR
                ne.name = unaccent(osm.name)
            )
          AND osm.place IN ('city', 'town', 'village')
          AND ST_DWithin(ne.geometry, osm.geometry, 50000)
    )
    UPDATE osm_city_point AS osm
        -- Move scalerank to range 1 to 10 and merge scalerank 5 with 6 since not enough cities
        -- are in the scalerank 5 bucket
    SET "rank" = CASE WHEN scalerank <= 5 THEN scalerank + 1 ELSE scalerank END
    FROM important_city_point AS ne
    WHERE (full_update OR osm.osm_id IN (SELECT osm_id FROM place_city.osm_ids))
      AND rank IS DISTINCT FROM CASE WHEN scalerank <= 5 THEN scalerank + 1 ELSE scalerank END
      AND osm.osm_id = ne.osm_id;

    UPDATE osm_city_point
    SET tags = update_tags(tags, geometry)
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM place_city.osm_ids))
      AND COALESCE(tags->'name:latin', tags->'name:nonlatin', tags->'name_int') IS NULL
      AND tags != update_tags(tags, geometry);

$$ LANGUAGE SQL;

SELECT update_osm_city_point(true);

CREATE INDEX IF NOT EXISTS osm_city_point_rank_idx ON osm_city_point ("rank");

-- Handle updates

CREATE OR REPLACE FUNCTION place_city.store() RETURNS trigger AS
$$
BEGIN
    IF (tg_op = 'DELETE') THEN
        INSERT INTO place_city.osm_ids VALUES (OLD.osm_id);
    ELSE
        INSERT INTO place_city.osm_ids VALUES (NEW.osm_id);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS place_city.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION place_city.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO place_city.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION place_city.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh place_city rank';
    PERFORM update_osm_city_point(false);
    -- noinspection SqlWithoutWhere
    DELETE FROM place_city.osm_ids;
    -- noinspection SqlWithoutWhere
    DELETE FROM place_city.updates;

    RAISE LOG 'Refresh place_city done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_store
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_city_point
    FOR EACH ROW
EXECUTE PROCEDURE place_city.store();

CREATE TRIGGER trigger_flag
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_city_point
    FOR EACH STATEMENT
EXECUTE PROCEDURE place_city.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON place_city.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE place_city.refresh();

-- Layer place - ./place.sql

-- etldoc: layer_place[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="layer_place | <z0_3> z0-3|<z4_7> z4-7|<z8_11> z8-11| <z12_14> z12-z14+" ] ;

CREATE OR REPLACE FUNCTION layer_place(bbox geometry, zoom_level int, pixel_width numeric)
    RETURNS TABLE
            (
                osm_id   bigint,
                geometry geometry,
                name     text,
                name_en  text,
                name_de  text,
                tags     hstore,
                class    text,
                "rank"   int,
                capital  int,
                iso_a2   text
            )
AS
$$
SELECT *
FROM (
         SELECT
             -- etldoc: osm_continent_point -> layer_place:z0_3
             osm_id * 10 AS osm_id,
             geometry,
             name,
             COALESCE(NULLIF(name_en, ''), name) AS name_en,
             COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
             tags,
             'continent' AS class,
             1 AS "rank",
             NULL::int AS capital,
             NULL::text AS iso_a2
         FROM osm_continent_point
         WHERE geometry && bbox
           AND zoom_level < 4

         UNION ALL

         SELECT
             -- etldoc: osm_country_point -> layer_place:z0_3
             -- etldoc: osm_country_point -> layer_place:z4_7
             -- etldoc: osm_country_point -> layer_place:z8_11
             -- etldoc: osm_country_point -> layer_place:z12_14
             osm_id * 10 AS osm_id,
             geometry,
             name,
             COALESCE(NULLIF(name_en, ''), name) AS name_en,
             COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
             tags,
             'country' AS class,
             "rank",
             NULL::int AS capital,
             iso3166_1_alpha_2 AS iso_a2
         FROM osm_country_point
         WHERE geometry && bbox
           AND "rank" <= zoom_level + 1
           AND name <> ''

         UNION ALL

         SELECT
             -- etldoc: osm_state_point  -> layer_place:z0_3
             -- etldoc: osm_state_point  -> layer_place:z4_7
             -- etldoc: osm_state_point  -> layer_place:z8_11
             -- etldoc: osm_state_point  -> layer_place:z12_14
             osm_id * 10 AS osm_id,
             geometry,
             name,
             COALESCE(NULLIF(name_en, ''), name) AS name_en,
             COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
             tags,
             'state' AS class,
             "rank",
             NULL::int AS capital,
             NULL::text AS iso_a2
         FROM osm_state_point
         WHERE geometry && bbox
           AND name <> ''
           AND zoom_level > 1

         UNION ALL

         SELECT
             -- etldoc: osm_island_point    -> layer_place:z12_14
             osm_id * 10 AS osm_id,
             geometry,
             name,
             COALESCE(NULLIF(name_en, ''), name) AS name_en,
             COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
             tags,
             'island' AS class,
             7 AS "rank",
             NULL::int AS capital,
             NULL::text AS iso_a2
         FROM osm_island_point
         WHERE zoom_level >= 12
           AND geometry && bbox

         UNION ALL

         SELECT
             -- etldoc: osm_island_polygon  -> layer_place:z8_11
             -- etldoc: osm_island_polygon  -> layer_place:z12_14
             osm_id * 10 AS osm_id,
             geometry,
             name,
             COALESCE(NULLIF(name_en, ''), name) AS name_en,
             COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
             tags,
             'island' AS class,
             island_rank(area) AS "rank",
             NULL::int AS capital,
             NULL::text AS iso_a2
         FROM osm_island_polygon
         WHERE geometry && bbox
           AND ((zoom_level = 8 AND island_rank(area) <= 3)
             OR (zoom_level = 9 AND island_rank(area) <= 4)
             OR (zoom_level >= 10))

         UNION ALL

         SELECT
             -- etldoc: layer_city          -> layer_place:z0_3
             -- etldoc: layer_city          -> layer_place:z4_7
             -- etldoc: layer_city          -> layer_place:z8_11
             -- etldoc: layer_city          -> layer_place:z12_14
             osm_id * 10 AS osm_id,
             geometry,
             name,
             name_en,
             name_de,
             tags,
             place::text AS class,
             "rank",
             capital,
             NULL::text AS iso_a2
         FROM layer_city(bbox, zoom_level, pixel_width)
         ORDER BY "rank" ASC
     ) AS place_all
$$ LANGUAGE SQL STABLE
                PARALLEL SAFE;
-- TODO: Check if the above can be made STRICT -- i.e. if pixel_width could be NULL

DO $$ BEGIN RAISE NOTICE 'Finished layer place'; END$$;
