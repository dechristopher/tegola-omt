DO $$ BEGIN RAISE NOTICE 'Processing layer water_name'; END$$;

-- Layer water_name - ./update_marine_point.sql

DROP TRIGGER IF EXISTS trigger_flag ON osm_marine_point;
DROP TRIGGER IF EXISTS trigger_store ON osm_marine_point;
DROP TRIGGER IF EXISTS trigger_refresh ON water_name_marine.updates;

CREATE SCHEMA IF NOT EXISTS water_name_marine;

CREATE TABLE IF NOT EXISTS water_name_marine.osm_ids
(
    osm_id bigint
);

CREATE OR REPLACE FUNCTION update_osm_marine_point(full_update boolean) RETURNS void AS
$$
    -- etldoc: ne_10m_geography_marine_polys -> osm_marine_point
    -- etldoc: osm_marine_point              -> osm_marine_point

    WITH important_marine_point AS (
        SELECT osm.osm_id, ne.scalerank
        FROM osm_marine_point AS osm
             LEFT JOIN ne_10m_geography_marine_polys AS ne ON
              lower(trim(regexp_replace(ne.name, '\\s+', ' ', 'g'))) IN (lower(osm.name), lower(osm.tags->'name:en'), lower(osm.tags->'name:es'))
           OR substring(lower(trim(regexp_replace(ne.name, '\\s+', ' ', 'g'))) FROM 1 FOR length(lower(osm.name))) = lower(osm.name)
    )
    UPDATE osm_marine_point AS osm
    SET "rank" = scalerank
    FROM important_marine_point AS ne
    WHERE (full_update OR osm.osm_id IN (SELECT osm_id FROM water_name_marine.osm_ids))
      AND osm.osm_id = ne.osm_id
      AND "rank" IS DISTINCT FROM scalerank;

    UPDATE osm_marine_point
    SET tags = update_tags(tags, geometry)
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM water_name_marine.osm_ids))
      AND COALESCE(tags->'name:latin', tags->'name:nonlatin', tags->'name_int') IS NULL
      AND tags != update_tags(tags, geometry);

$$ LANGUAGE SQL;

SELECT update_osm_marine_point(true);

CREATE INDEX IF NOT EXISTS osm_marine_point_rank_idx ON osm_marine_point ("rank");

-- Handle updates

CREATE OR REPLACE FUNCTION water_name_marine.store() RETURNS trigger AS
$$
BEGIN
    IF (tg_op = 'DELETE') THEN
        INSERT INTO water_name_marine.osm_ids VALUES (OLD.osm_id);
    ELSE
        INSERT INTO water_name_marine.osm_ids VALUES (NEW.osm_id);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS water_name_marine.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION water_name_marine.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO water_name_marine.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION water_name_marine.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh water_name_marine rank';
    PERFORM update_osm_marine_point(false);
    -- noinspection SqlWithoutWhere
    DELETE FROM water_name_marine.osm_ids;
    -- noinspection SqlWithoutWhere
    DELETE FROM water_name_marine.updates;

    RAISE LOG 'Refresh water_name_marine done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_store
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_marine_point
    FOR EACH ROW
EXECUTE PROCEDURE water_name_marine.store();

CREATE TRIGGER trigger_flag
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_marine_point
    FOR EACH STATEMENT
EXECUTE PROCEDURE water_name_marine.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON water_name_marine.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE water_name_marine.refresh();

-- Layer water_name - ./update_water_lakeline.sql

DROP TRIGGER IF EXISTS trigger_delete_line ON osm_water_polygon;
DROP TRIGGER IF EXISTS trigger_update_line ON osm_water_polygon;
DROP TRIGGER IF EXISTS trigger_insert_line ON osm_water_polygon;

CREATE OR REPLACE VIEW osm_water_lakeline_view AS
SELECT wp.osm_id,
       ll.wkb_geometry AS geometry,
       name,
       name_en,
       name_de,
       update_tags(tags, ll.wkb_geometry) AS tags,
       ST_Area(wp.geometry) AS area,
       is_intermittent
FROM osm_water_polygon AS wp
         INNER JOIN lake_centerline ll ON wp.osm_id = ll.osm_id
WHERE wp.name <> ''
  AND ST_IsValid(wp.geometry);

-- etldoc:  osm_water_polygon ->  osm_water_lakeline
-- etldoc:  lake_centerline  ->  osm_water_lakeline
CREATE TABLE IF NOT EXISTS osm_water_lakeline AS
SELECT *
FROM osm_water_lakeline_view;
DO
$$
    BEGIN
        ALTER TABLE osm_water_lakeline
            ADD CONSTRAINT osm_water_lakeline_pk PRIMARY KEY (osm_id);
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'primary key osm_water_lakeline_pk already exists in osm_water_lakeline.';
    END;
$$;
CREATE INDEX IF NOT EXISTS osm_water_lakeline_geometry_idx ON osm_water_lakeline USING gist (geometry);

-- Handle updates

CREATE SCHEMA IF NOT EXISTS water_lakeline;

CREATE OR REPLACE FUNCTION water_lakeline.delete() RETURNS trigger AS
$$
BEGIN
    DELETE
    FROM osm_water_lakeline
    WHERE osm_water_lakeline.osm_id = OLD.osm_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION water_lakeline.update() RETURNS trigger AS
$$
BEGIN
    UPDATE osm_water_lakeline
    SET (osm_id, geometry, name, name_en, name_de, tags, area, is_intermittent) =
            (SELECT * FROM osm_water_lakeline_view WHERE osm_water_lakeline_view.osm_id = NEW.osm_id)
    WHERE osm_water_lakeline.osm_id = NEW.osm_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION water_lakeline.insert() RETURNS trigger AS
$$
BEGIN
    INSERT INTO osm_water_lakeline
    SELECT *
    FROM osm_water_lakeline_view
    WHERE osm_water_lakeline_view.osm_id = NEW.osm_id
    -- May happen in case we replay update
    ON CONFLICT ON CONSTRAINT osm_water_lakeline_pk
    DO NOTHING;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_delete_line
    AFTER DELETE
    ON osm_water_polygon
    FOR EACH ROW
EXECUTE PROCEDURE water_lakeline.delete();

CREATE TRIGGER trigger_update_line
    AFTER UPDATE
    ON osm_water_polygon
    FOR EACH ROW
EXECUTE PROCEDURE water_lakeline.update();

CREATE TRIGGER trigger_insert_line
    AFTER INSERT
    ON osm_water_polygon
    FOR EACH ROW
EXECUTE PROCEDURE water_lakeline.insert();

-- Layer water_name - ./update_water_point.sql

DROP TRIGGER IF EXISTS trigger_delete_point ON osm_water_polygon;
DROP TRIGGER IF EXISTS trigger_update_point ON osm_water_polygon;
DROP TRIGGER IF EXISTS trigger_insert_point ON osm_water_polygon;

CREATE OR REPLACE VIEW osm_water_point_view AS
SELECT wp.osm_id,
       ST_PointOnSurface(wp.geometry) AS geometry,
       wp.name,
       wp.name_en,
       wp.name_de,
       update_tags(wp.tags, ST_PointOnSurface(wp.geometry)) AS tags,
       ST_Area(wp.geometry) AS area,
       wp.is_intermittent
FROM osm_water_polygon AS wp
         LEFT JOIN lake_centerline ll ON wp.osm_id = ll.osm_id
WHERE ll.osm_id IS NULL
  AND wp.name <> ''
  AND ST_IsValid(wp.geometry);

-- etldoc:  osm_water_polygon ->  osm_water_point
-- etldoc:  lake_centerline ->  osm_water_point
CREATE TABLE IF NOT EXISTS osm_water_point AS
SELECT *
FROM osm_water_point_view;
DO
$$
    BEGIN
        ALTER TABLE osm_water_point
            ADD CONSTRAINT osm_water_point_pk PRIMARY KEY (osm_id);
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'primary key osm_water_point_pk already exists in osm_water_point.';
    END;
$$;
CREATE INDEX IF NOT EXISTS osm_water_point_geometry_idx ON osm_water_point USING gist (geometry);

-- Handle updates

CREATE SCHEMA IF NOT EXISTS water_point;

CREATE OR REPLACE FUNCTION water_point.delete() RETURNS trigger AS
$$
BEGIN
    DELETE
    FROM osm_water_point
    WHERE osm_water_point.osm_id = OLD.osm_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION water_point.update() RETURNS trigger AS
$$
BEGIN
    UPDATE osm_water_point
    SET (osm_id, geometry, name, name_en, name_de, tags, area, is_intermittent) =
            (SELECT * FROM osm_water_point_view WHERE osm_water_point_view.osm_id = NEW.osm_id)
    WHERE osm_water_point.osm_id = NEW.osm_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION water_point.insert() RETURNS trigger AS
$$
BEGIN
    INSERT INTO osm_water_point
    SELECT *
    FROM osm_water_point_view
    WHERE osm_water_point_view.osm_id = NEW.osm_id
    -- May happen in case we replay update
    ON CONFLICT ON CONSTRAINT osm_water_point_pk
    DO NOTHING;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_delete_point
    AFTER DELETE
    ON osm_water_polygon
    FOR EACH ROW
EXECUTE PROCEDURE water_point.delete();

CREATE TRIGGER trigger_update_point
    AFTER UPDATE
    ON osm_water_polygon
    FOR EACH ROW
EXECUTE PROCEDURE water_point.update();

CREATE TRIGGER trigger_insert_point
    AFTER INSERT
    ON osm_water_polygon
    FOR EACH ROW
EXECUTE PROCEDURE water_point.insert();

-- Layer water_name - ./water_name.sql

-- etldoc: layer_water_name[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="layer_water_name | <z0_8> z0_8 | <z9_13> z9_13 | <z14_> z14+" ] ;

CREATE OR REPLACE FUNCTION layer_water_name(bbox geometry, zoom_level integer)
    RETURNS TABLE
            (
                osm_id       bigint,
                geometry     geometry,
                name         text,
                name_en      text,
                name_de      text,
                tags         hstore,
                class        text,
                intermittent int
            )
AS
$$
SELECT
    -- etldoc: osm_water_lakeline ->  layer_water_name:z9_13
    -- etldoc: osm_water_lakeline ->  layer_water_name:z14_
    CASE
        WHEN osm_id < 0 THEN -osm_id * 10 + 4
        ELSE osm_id * 10 + 1
        END AS osm_id_hash,
    geometry,
    name,
    COALESCE(NULLIF(name_en, ''), name) AS name_en,
    COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
    tags,
    'lake'::text AS class,
    is_intermittent::int AS intermittent
FROM osm_water_lakeline
WHERE geometry && bbox
  AND ((zoom_level BETWEEN 9 AND 13 AND LineLabel(zoom_level, NULLIF(name, ''), geometry))
    OR (zoom_level >= 14))
UNION ALL
SELECT
    -- etldoc: osm_water_point ->  layer_water_name:z9_13
    -- etldoc: osm_water_point ->  layer_water_name:z14_
    CASE
        WHEN osm_id < 0 THEN -osm_id * 10 + 4
        ELSE osm_id * 10 + 1
        END AS osm_id_hash,
    geometry,
    name,
    COALESCE(NULLIF(name_en, ''), name) AS name_en,
    COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
    tags,
    'lake'::text AS class,
    is_intermittent::int AS intermittent
FROM osm_water_point
WHERE geometry && bbox
  AND (
        (zoom_level BETWEEN 9 AND 13 AND area > 70000 * 2 ^ (20 - zoom_level))
        OR (zoom_level >= 14)
    )
UNION ALL
SELECT
    -- etldoc: osm_marine_point ->  layer_water_name:z0_8
    -- etldoc: osm_marine_point ->  layer_water_name:z9_13
    -- etldoc: osm_marine_point ->  layer_water_name:z14_
    osm_id * 10 AS osm_id_hash,
    geometry,
    name,
    COALESCE(NULLIF(name_en, ''), name) AS name_en,
    COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
    tags,
    place::text AS class,
    is_intermittent::int AS intermittent
FROM osm_marine_point
WHERE geometry && bbox
  AND (
        place = 'ocean'
        OR (zoom_level >= "rank" AND "rank" IS NOT NULL)
        OR (zoom_level >= 8)
    );
$$ LANGUAGE SQL STABLE
                -- STRICT
                PARALLEL SAFE;

DO $$ BEGIN RAISE NOTICE 'Finished layer water_name'; END$$;
