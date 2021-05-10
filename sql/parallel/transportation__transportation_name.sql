DO $$ BEGIN RAISE NOTICE 'Processing layer transportation'; END$$;

-- Layer transportation - ./class.sql

CREATE OR REPLACE FUNCTION brunnel(is_bridge bool, is_tunnel bool, is_ford bool) RETURNS text AS
$$
SELECT CASE
           WHEN is_bridge THEN 'bridge'
           WHEN is_tunnel THEN 'tunnel'
           WHEN is_ford THEN 'ford'
           END;
$$ LANGUAGE SQL IMMUTABLE
                STRICT
                PARALLEL SAFE;

-- The classes for highways are derived from the classes used in ClearTables
-- https://github.com/ClearTables/ClearTables/blob/master/transportation.lua
CREATE OR REPLACE FUNCTION highway_class(highway text, public_transport text, construction text) RETURNS text AS
$$
SELECT CASE
           WHEN "highway" IN ('motorway', 'motorway_link') THEN 'motorway'
           WHEN "highway" IN ('trunk', 'trunk_link') THEN 'trunk'
           WHEN "highway" IN ('primary', 'primary_link') THEN 'primary'
           WHEN "highway" IN ('secondary', 'secondary_link') THEN 'secondary'
           WHEN "highway" IN ('tertiary', 'tertiary_link') THEN 'tertiary'
           WHEN "highway" IN ('unclassified', 'residential', 'living_street', 'road') THEN 'minor'
           WHEN "highway" IN ('pedestrian', 'path', 'footway', 'cycleway', 'steps', 'bridleway', 'corridor')
               OR "public_transport" = 'platform'
               THEN 'path'
           WHEN "highway" = 'service' THEN 'service'
           WHEN "highway" = 'track' THEN 'track'
           WHEN "highway" = 'raceway' THEN 'raceway'
           WHEN "highway" = 'construction'
               AND "construction" IN ('motorway', 'motorway_link')
               THEN 'motorway_construction'
           WHEN "highway" = 'construction'
               AND "construction" IN ('trunk', 'trunk_link')
               THEN 'trunk_construction'
           WHEN "highway" = 'construction'
               AND "construction" IN ('primary', 'primary_link')
               THEN 'primary_construction'
           WHEN "highway" = 'construction'
               AND "construction" IN ('secondary', 'secondary_link')
               THEN 'secondary_construction'
           WHEN "highway" = 'construction'
               AND "construction" IN ('tertiary', 'tertiary_link')
               THEN 'tertiary_construction'
           WHEN "highway" = 'construction'
               AND "construction" IN ('', 'unclassified', 'residential', 'living_street', 'road')
               THEN 'minor_construction'
           WHEN "highway" = 'construction'
               AND ("construction" IN ('pedestrian', 'path', 'footway', 'cycleway', 'steps', 'bridleway', 'corridor') OR "public_transport" = 'platform')
               THEN 'path_construction'
           WHEN "highway" = 'construction'
               AND "construction" = 'service'
               THEN 'service_construction'
           WHEN "highway" = 'construction'
               AND "construction" = 'track'
               THEN 'track_construction'
           WHEN "highway" = 'construction'
               AND "construction" = 'raceway'
               THEN 'raceway_construction'
           END;
$$ LANGUAGE SQL IMMUTABLE
                PARALLEL SAFE;

-- The classes for railways are derived from the classes used in ClearTables
-- https://github.com/ClearTables/ClearTables/blob/master/transportation.lua
CREATE OR REPLACE FUNCTION railway_class(railway text) RETURNS text AS
$$
SELECT CASE
           WHEN railway IN ('rail', 'narrow_gauge', 'preserved', 'funicular') THEN 'rail'
           WHEN railway IN ('subway', 'light_rail', 'monorail', 'tram') THEN 'transit'
           END;
$$ LANGUAGE SQL IMMUTABLE
                STRICT
                PARALLEL SAFE;

-- Limit service to only the most important values to ensure
-- we always know the values of service
CREATE OR REPLACE FUNCTION service_value(service text) RETURNS text AS
$$
SELECT CASE
           WHEN service IN ('spur', 'yard', 'siding', 'crossover', 'driveway', 'alley', 'parking_aisle') THEN service
           END;
$$ LANGUAGE SQL IMMUTABLE
                STRICT
                PARALLEL SAFE;

-- Limit surface to only the most important values to ensure
-- we always know the values of surface
CREATE OR REPLACE FUNCTION surface_value(surface text) RETURNS text AS
$$
SELECT CASE
           WHEN surface IN ('paved', 'asphalt', 'cobblestone', 'concrete', 'concrete:lanes', 'concrete:plates', 'metal',
                            'paving_stones', 'sett', 'unhewn_cobblestone', 'wood') THEN 'paved'
           WHEN surface IN ('unpaved', 'compacted', 'dirt', 'earth', 'fine_gravel', 'grass', 'grass_paver', 'gravel',
                            'gravel_turf', 'ground', 'ice', 'mud', 'pebblestone', 'salt', 'sand', 'snow', 'woodchips')
               THEN 'unpaved'
           END;
$$ LANGUAGE SQL IMMUTABLE
                STRICT
                PARALLEL SAFE;

-- Layer transportation - ./update_transportation_merge.sql

DROP TRIGGER IF EXISTS trigger_flag_transportation ON osm_highway_linestring;
DROP TRIGGER IF EXISTS trigger_refresh ON transportation.updates;

-- Instead of using relations to find out the road names we
-- stitch together the touching ways with the same name
-- to allow for nice label rendering
-- Because this works well for roads that do not have relations as well


-- Improve performance of the sql in transportation_name/network_type.sql
CREATE INDEX IF NOT EXISTS osm_highway_linestring_highway_partial_idx
    ON osm_highway_linestring (highway)
    WHERE highway IN ('motorway', 'trunk', 'primary', 'construction');

-- etldoc: osm_highway_linestring ->  osm_transportation_merge_linestring
DROP MATERIALIZED VIEW IF EXISTS osm_transportation_merge_linestring CASCADE;
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring AS
(
SELECT (ST_Dump(geometry)).geom AS geometry,
       NULL::bigint AS osm_id,
       highway,
       construction,
       is_bridge,
       is_tunnel,
       is_ford,
       z_order
FROM (
         SELECT ST_LineMerge(ST_Collect(geometry)) AS geometry,
                highway,
                construction,
                is_bridge,
                is_tunnel,
                is_ford,
                min(z_order) AS z_order
         FROM osm_highway_linestring
         WHERE (highway IN ('motorway', 'trunk', 'primary') OR
                highway = 'construction' AND construction IN ('motorway', 'trunk', 'primary'))
           AND ST_IsValid(geometry)
         GROUP BY highway, construction, is_bridge, is_tunnel, is_ford
     ) AS highway_union
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_geometry_idx
    ON osm_transportation_merge_linestring USING gist (geometry);
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_highway_partial_idx
    ON osm_transportation_merge_linestring (highway, construction)
    WHERE highway IN ('motorway', 'trunk', 'primary', 'construction');

-- etldoc: osm_transportation_merge_linestring -> osm_transportation_merge_linestring_gen_z8
DROP MATERIALIZED VIEW IF EXISTS osm_transportation_merge_linestring_gen_z8 CASCADE;
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z8 AS
(
SELECT ST_Simplify(geometry, ZRes(10)) AS geometry,
       osm_id,
       highway,
       construction,
       is_bridge,
       is_tunnel,
       is_ford,
       z_order
FROM osm_transportation_merge_linestring
WHERE highway IN ('motorway', 'trunk', 'primary')
   OR highway = 'construction' AND construction IN ('motorway', 'trunk', 'primary')
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z8_geometry_idx
    ON osm_transportation_merge_linestring_gen_z8 USING gist (geometry);
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z8_highway_partial_idx
    ON osm_transportation_merge_linestring_gen_z8 (highway, construction)
    WHERE highway IN ('motorway', 'trunk', 'primary', 'construction');

-- etldoc: osm_transportation_merge_linestring_gen_z8 -> osm_transportation_merge_linestring_gen_z7
DROP MATERIALIZED VIEW IF EXISTS osm_transportation_merge_linestring_gen_z7 CASCADE;
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z7 AS
(
SELECT ST_Simplify(geometry, ZRes(9)) AS geometry,
       osm_id,
       highway,
       construction,
       is_bridge,
       is_tunnel,
       is_ford,
       z_order
FROM osm_transportation_merge_linestring_gen_z8
WHERE (highway IN ('motorway', 'trunk', 'primary') OR
       highway = 'construction' AND construction IN ('motorway', 'trunk', 'primary'))
  AND ST_Length(geometry) > 50
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z7_geometry_idx
    ON osm_transportation_merge_linestring_gen_z7 USING gist (geometry);
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z7_highway_partial_idx
    ON osm_transportation_merge_linestring_gen_z7 (highway, construction)
    WHERE highway IN ('motorway', 'trunk', 'primary', 'construction');

-- etldoc: osm_transportation_merge_linestring_gen_z7 -> osm_transportation_merge_linestring_gen_z6
DROP MATERIALIZED VIEW IF EXISTS osm_transportation_merge_linestring_gen_z6 CASCADE;
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z6 AS
(
SELECT ST_Simplify(geometry, ZRes(8)) AS geometry,
       osm_id,
       highway,
       construction,
       is_bridge,
       is_tunnel,
       is_ford,
       z_order
FROM osm_transportation_merge_linestring_gen_z7
WHERE (highway IN ('motorway', 'trunk') OR highway = 'construction' AND construction IN ('motorway', 'trunk'))
  AND ST_Length(geometry) > 100
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z6_geometry_idx
    ON osm_transportation_merge_linestring_gen_z6 USING gist (geometry);
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z6_highway_partial_idx
    ON osm_transportation_merge_linestring_gen_z6 (highway, construction)
    WHERE highway IN ('motorway', 'trunk', 'construction');

-- etldoc: osm_transportation_merge_linestring_gen_z6 -> osm_transportation_merge_linestring_gen_z5
DROP MATERIALIZED VIEW IF EXISTS osm_transportation_merge_linestring_gen_z5 CASCADE;
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z5 AS
(
SELECT ST_Simplify(geometry, ZRes(7)) AS geometry,
       osm_id,
       highway,
       construction,
       is_bridge,
       is_tunnel,
       is_ford,
       z_order
FROM osm_transportation_merge_linestring_gen_z6
WHERE (highway IN ('motorway', 'trunk') OR highway = 'construction' AND construction IN ('motorway', 'trunk'))
  AND ST_Length(geometry) > 500
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z5_geometry_idx
    ON osm_transportation_merge_linestring_gen_z5 USING gist (geometry);
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z5_highway_partial_idx
    ON osm_transportation_merge_linestring_gen_z5 (highway, construction)
    WHERE highway IN ('motorway', 'trunk', 'construction');

-- etldoc: osm_transportation_merge_linestring_gen_z5 -> osm_transportation_merge_linestring_gen_z4
DROP MATERIALIZED VIEW IF EXISTS osm_transportation_merge_linestring_gen_z4 CASCADE;
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z4 AS
(
SELECT ST_Simplify(geometry, ZRes(6)) AS geometry,
       osm_id,
       highway,
       construction,
       is_bridge,
       is_tunnel,
       is_ford,
       z_order
FROM osm_transportation_merge_linestring_gen_z5
WHERE (highway = 'motorway' OR highway = 'construction' AND construction = 'motorway')
  AND ST_Length(geometry) > 1000
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z4_geometry_idx
    ON osm_transportation_merge_linestring_gen_z4 USING gist (geometry);


-- Handle updates

CREATE SCHEMA IF NOT EXISTS transportation;

CREATE TABLE IF NOT EXISTS transportation.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION transportation.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO transportation.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION transportation.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh transportation';
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring;
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z8;
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z7;
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z6;
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z5;
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z4;
    -- noinspection SqlWithoutWhere
    DELETE FROM transportation.updates;

    RAISE LOG 'Refresh transportation done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_flag_transportation
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_highway_linestring
    FOR EACH STATEMENT
EXECUTE PROCEDURE transportation.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON transportation.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE transportation.refresh();

-- Layer transportation - ./transportation.sql

CREATE OR REPLACE FUNCTION highway_is_link(highway text) RETURNS boolean AS
$$
SELECT highway LIKE '%_link';
$$ LANGUAGE SQL IMMUTABLE
                STRICT
                PARALLEL SAFE;


-- etldoc: layer_transportation[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="<sql> layer_transportation |<z4> z4 |<z5> z5 |<z6> z6 |<z7> z7 |<z8> z8 |<z9> z9 |<z10> z10 |<z11> z11 |<z12> z12|<z13> z13|<z14_> z14+" ] ;
CREATE OR REPLACE FUNCTION layer_transportation(bbox geometry, zoom_level int)
    RETURNS TABLE
            (
                osm_id    bigint,
                geometry  geometry,
                class     text,
                subclass  text,
                ramp      int,
                oneway    int,
                brunnel   text,
                service   text,
                layer     int,
                level     int,
                indoor    int,
                bicycle   text,
                foot      text,
                horse     text,
                mtb_scale text,
                surface   text
            )
AS
$$
SELECT osm_id,
       geometry,
       CASE
           WHEN NULLIF(highway, '') IS NOT NULL OR NULLIF(public_transport, '') IS NOT NULL
               THEN highway_class(highway, public_transport, construction)
           WHEN NULLIF(railway, '') IS NOT NULL THEN railway_class(railway)
           WHEN NULLIF(aerialway, '') IS NOT NULL THEN 'aerialway'
           WHEN NULLIF(shipway, '') IS NOT NULL THEN shipway
           WHEN NULLIF(man_made, '') IS NOT NULL THEN man_made
           END AS class,
       CASE
           WHEN railway IS NOT NULL THEN railway
           WHEN (highway IS NOT NULL OR public_transport IS NOT NULL)
               AND highway_class(highway, public_transport, construction) = 'path'
               THEN COALESCE(NULLIF(public_transport, ''), highway)
           WHEN aerialway IS NOT NULL THEN aerialway
           END AS subclass,
       -- All links are considered as ramps as well
       CASE
           WHEN highway_is_link(highway) OR highway = 'steps'
               THEN 1
           ELSE is_ramp::int END AS ramp,
       is_oneway::int AS oneway,
       brunnel(is_bridge, is_tunnel, is_ford) AS brunnel,
       NULLIF(service, '') AS service,
       NULLIF(layer, 0) AS layer,
       "level",
       CASE WHEN indoor = TRUE THEN 1 END AS indoor,
       NULLIF(bicycle, '') AS bicycle,
       NULLIF(foot, '') AS foot,
       NULLIF(horse, '') AS horse,
       NULLIF(mtb_scale, '') AS mtb_scale,
       NULLIF(surface, '') AS surface
FROM (
         -- etldoc: osm_transportation_merge_linestring_gen_z4 -> layer_transportation:z4
         SELECT osm_id,
                geometry,
                highway,
                construction,
                NULL AS railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                NULL AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                NULL::boolean AS is_ramp,
                NULL::int AS is_oneway,
                NULL AS man_made,
                NULL::int AS layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_transportation_merge_linestring_gen_z4
         WHERE zoom_level = 4
         UNION ALL

         -- etldoc: osm_transportation_merge_linestring_gen_z5 -> layer_transportation:z5
         SELECT osm_id,
                geometry,
                highway,
                construction,
                NULL AS railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                NULL AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                NULL::boolean AS is_ramp,
                NULL::int AS is_oneway,
                NULL AS man_made,
                NULL::int AS layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_transportation_merge_linestring_gen_z5
         WHERE zoom_level = 5
         UNION ALL

         -- etldoc: osm_transportation_merge_linestring_gen_z6 -> layer_transportation:z6
         SELECT osm_id,
                geometry,
                highway,
                construction,
                NULL AS railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                NULL AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                NULL::boolean AS is_ramp,
                NULL::int AS is_oneway,
                NULL AS man_made,
                NULL::int AS layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_transportation_merge_linestring_gen_z6
         WHERE zoom_level = 6
         UNION ALL

         -- etldoc: osm_transportation_merge_linestring_gen_z7  ->  layer_transportation:z7
         SELECT osm_id,
                geometry,
                highway,
                construction,
                NULL AS railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                NULL AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                NULL::boolean AS is_ramp,
                NULL::int AS is_oneway,
                NULL AS man_made,
                NULL::int AS layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_transportation_merge_linestring_gen_z7
         WHERE zoom_level = 7
         UNION ALL

         -- etldoc: osm_transportation_merge_linestring_gen_z8  ->  layer_transportation:z8
         SELECT osm_id,
                geometry,
                highway,
                construction,
                NULL AS railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                NULL AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                NULL::boolean AS is_ramp,
                NULL::int AS is_oneway,
                NULL AS man_made,
                NULL::int AS layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_transportation_merge_linestring_gen_z8
         WHERE zoom_level = 8
         UNION ALL

         -- etldoc: osm_highway_linestring_gen_z9  ->  layer_transportation:z9
         SELECT osm_id,
                geometry,
                highway,
                construction,
                NULL AS railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                NULL AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                NULL::boolean AS is_ramp,
                NULL::int AS is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                bicycle,
                foot,
                horse,
                mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_highway_linestring_gen_z9
         WHERE zoom_level = 9
           AND ST_Length(geometry) > ZRes(11)
         UNION ALL

         -- etldoc: osm_highway_linestring_gen_z10  ->  layer_transportation:z10
         SELECT osm_id,
                geometry,
                highway,
                construction,
                NULL AS railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                NULL AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                NULL::boolean AS is_ramp,
                NULL::int AS is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                bicycle,
                foot,
                horse,
                mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_highway_linestring_gen_z10
         WHERE zoom_level = 10
           AND ST_Length(geometry) > ZRes(11)
         UNION ALL

         -- etldoc: osm_highway_linestring_gen_z11  ->  layer_transportation:z11
         SELECT osm_id,
                geometry,
                highway,
                construction,
                NULL AS railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                NULL AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                NULL::boolean AS is_ramp,
                NULL::int AS is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                bicycle,
                foot,
                horse,
                mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_highway_linestring_gen_z11
         WHERE zoom_level = 11
           AND ST_Length(geometry) > ZRes(12)
         UNION ALL

         -- etldoc: osm_highway_linestring  ->  layer_transportation:z12
         -- etldoc: osm_highway_linestring  ->  layer_transportation:z13
         -- etldoc: osm_highway_linestring  ->  layer_transportation:z14_
         SELECT osm_id,
                geometry,
                highway,
                construction,
                NULL AS railway,
                NULL AS aerialway,
                NULL AS shipway,
                public_transport,
                service_value(service) AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                is_ramp,
                is_oneway,
                man_made,
                layer,
                CASE WHEN highway IN ('footway', 'steps') THEN "level" END AS "level",
                CASE WHEN highway IN ('footway', 'steps') THEN indoor END AS indoor,
                bicycle,
                foot,
                horse,
                mtb_scale,
                surface_value(surface) AS "surface",
                z_order
         FROM osm_highway_linestring
         WHERE NOT is_area
           AND (
                     zoom_level = 12 AND (
                             highway_class(highway, public_transport, construction) NOT IN ('track', 'path', 'minor')
                         OR highway IN ('unclassified', 'residential')
                     ) AND man_made <> 'pier'
                 OR zoom_level = 13
                         AND (
                                    highway_class(highway, public_transport, construction) NOT IN ('track', 'path') AND
                                    man_made <> 'pier'
                            OR
                                    man_made = 'pier' AND NOT ST_IsClosed(geometry)
                        )
                 OR zoom_level >= 14
                         AND (
                            man_made <> 'pier'
                            OR
                            NOT ST_IsClosed(geometry)
                        )
             )
         UNION ALL

         -- etldoc: osm_railway_linestring_gen_z8  ->  layer_transportation:z8
         SELECT osm_id,
                geometry,
                NULL AS highway,
                NULL AS construction,
                railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                service_value(service) AS service,
                NULL::boolean AS is_bridge,
                NULL::boolean AS is_tunnel,
                NULL::boolean AS is_ford,
                NULL::boolean AS is_ramp,
                NULL::int AS is_oneway,
                NULL AS man_made,
                NULL::int AS layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_railway_linestring_gen_z8
         WHERE zoom_level = 8
           AND railway = 'rail'
           AND service = ''
           AND usage = 'main'
         UNION ALL

         -- etldoc: osm_railway_linestring_gen_z9  ->  layer_transportation:z9
         SELECT osm_id,
                geometry,
                NULL AS highway,
                NULL AS construction,
                railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                service_value(service) AS service,
                NULL::boolean AS is_bridge,
                NULL::boolean AS is_tunnel,
                NULL::boolean AS is_ford,
                NULL::boolean AS is_ramp,
                NULL::int AS is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_railway_linestring_gen_z9
         WHERE zoom_level = 9
           AND railway = 'rail'
           AND service = ''
           AND usage = 'main'
         UNION ALL

         -- etldoc: osm_railway_linestring_gen_z10  ->  layer_transportation:z10
         SELECT osm_id,
                geometry,
                NULL AS highway,
                NULL AS construction,
                railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                service_value(service) AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                is_ramp,
                is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_railway_linestring_gen_z10
         WHERE zoom_level = 10
           AND railway IN ('rail', 'narrow_gauge')
           AND service = ''
         UNION ALL

         -- etldoc: osm_railway_linestring_gen_z11  ->  layer_transportation:z11
         SELECT osm_id,
                geometry,
                NULL AS highway,
                NULL AS construction,
                railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                service_value(service) AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                is_ramp,
                is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_railway_linestring_gen_z11
         WHERE zoom_level = 11
           AND railway IN ('rail', 'narrow_gauge', 'light_rail')
           AND service = ''
         UNION ALL

         -- etldoc: osm_railway_linestring_gen_z12  ->  layer_transportation:z12
         SELECT osm_id,
                geometry,
                NULL AS highway,
                NULL AS construction,
                railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                service_value(service) AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                is_ramp,
                is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_railway_linestring_gen_z12
         WHERE zoom_level = 12
           AND railway IN ('rail', 'narrow_gauge', 'light_rail')
           AND service = ''
         UNION ALL

         -- etldoc: osm_railway_linestring ->  layer_transportation:z13
         -- etldoc: osm_railway_linestring ->  layer_transportation:z14_
         SELECT osm_id,
                geometry,
                NULL AS highway,
                NULL AS construction,
                railway,
                NULL AS aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                service_value(service) AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                is_ramp,
                is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_railway_linestring
         WHERE zoom_level = 13
           AND railway IN ('rail', 'narrow_gauge', 'light_rail')
           AND service = ''
           OR zoom_level >= 14
         UNION ALL

         -- etldoc: osm_aerialway_linestring_gen_z12  ->  layer_transportation:z12
         SELECT osm_id,
                geometry,
                NULL AS highway,
                NULL AS construction,
                NULL AS railway,
                aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                service_value(service) AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                is_ramp,
                is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_aerialway_linestring_gen_z12
         WHERE zoom_level = 12
         UNION ALL

         -- etldoc: osm_aerialway_linestring ->  layer_transportation:z13
         -- etldoc: osm_aerialway_linestring ->  layer_transportation:z14_
         SELECT osm_id,
                geometry,
                NULL AS highway,
                NULL AS construction,
                NULL AS railway,
                aerialway,
                NULL AS shipway,
                NULL AS public_transport,
                service_value(service) AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                is_ramp,
                is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_aerialway_linestring
         WHERE zoom_level >= 13
         UNION ALL

         -- etldoc: osm_shipway_linestring_gen_z11  ->  layer_transportation:z11
         SELECT osm_id,
                geometry,
                NULL AS highway,
                NULL AS construction,
                NULL AS railway,
                NULL AS aerialway,
                shipway,
                NULL AS public_transport,
                service_value(service) AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                is_ramp,
                is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_shipway_linestring_gen_z11
         WHERE zoom_level = 11
         UNION ALL

         -- etldoc: osm_shipway_linestring_gen_z12  ->  layer_transportation:z12
         SELECT osm_id,
                geometry,
                NULL AS highway,
                NULL AS construction,
                NULL AS railway,
                NULL AS aerialway,
                shipway,
                NULL AS public_transport,
                service_value(service) AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                is_ramp,
                is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_shipway_linestring_gen_z12
         WHERE zoom_level = 12
         UNION ALL

         -- etldoc: osm_shipway_linestring ->  layer_transportation:z13
         -- etldoc: osm_shipway_linestring ->  layer_transportation:z14_
         SELECT osm_id,
                geometry,
                NULL AS highway,
                NULL AS construction,
                NULL AS railway,
                NULL AS aerialway,
                shipway,
                NULL AS public_transport,
                service_value(service) AS service,
                is_bridge,
                is_tunnel,
                is_ford,
                is_ramp,
                is_oneway,
                NULL AS man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_shipway_linestring
         WHERE zoom_level >= 13
         UNION ALL

         -- NOTE: We limit the selection of polys because we need to be
         -- careful to net get false positives here because
         -- it is possible that closed linestrings appear both as
         -- highway linestrings and as polygon
         -- etldoc: osm_highway_polygon ->  layer_transportation:z13
         -- etldoc: osm_highway_polygon ->  layer_transportation:z14_
         SELECT osm_id,
                geometry,
                highway,
                NULL AS construction,
                NULL AS railway,
                NULL AS aerialway,
                NULL AS shipway,
                public_transport,
                NULL AS service,
                CASE
                    WHEN man_made IN ('bridge') THEN TRUE
                    ELSE FALSE
                    END AS is_bridge,
                FALSE AS is_tunnel,
                FALSE AS is_ford,
                FALSE AS is_ramp,
                FALSE::int AS is_oneway,
                man_made,
                layer,
                NULL::int AS level,
                NULL::boolean AS indoor,
                NULL AS bicycle,
                NULL AS foot,
                NULL AS horse,
                NULL AS mtb_scale,
                NULL AS surface,
                z_order
         FROM osm_highway_polygon
              -- We do not want underground pedestrian areas for now
         WHERE zoom_level >= 13
           AND (
                 man_made IN ('bridge', 'pier')
                 OR (is_area AND COALESCE(layer, 0) >= 0)
             )
     ) AS zoom_levels
WHERE geometry && bbox
ORDER BY z_order ASC;
$$ LANGUAGE SQL STABLE
                -- STRICT
                PARALLEL SAFE;

DO $$ BEGIN RAISE NOTICE 'Finished layer transportation'; END$$;

DO $$ BEGIN RAISE NOTICE 'Processing layer transportation_name'; END$$;

-- Layer transportation_name - ./network_type.sql

DROP TRIGGER IF EXISTS trigger_store_transportation_route_member ON osm_route_member;
DROP TRIGGER IF EXISTS trigger_store_transportation_highway_linestring ON osm_highway_linestring;
DROP TRIGGER IF EXISTS trigger_flag_transportation_name ON transportation_name.network_changes;
DROP TRIGGER IF EXISTS trigger_refresh_network ON transportation_name.updates_network;

DROP TRIGGER IF EXISTS trigger_store_transportation_name_network ON osm_transportation_name_network;
DROP TRIGGER IF EXISTS trigger_flag_name ON transportation_name.name_changes;
DROP TRIGGER IF EXISTS trigger_refresh_name ON transportation_name.updates_name;

DO
$$
    BEGIN
        IF NOT EXISTS(SELECT 1 FROM pg_type WHERE typname = 'route_network_type') THEN
            CREATE TYPE route_network_type AS enum (
                'us-interstate', 'us-highway', 'us-state',
                'ca-transcanada',
                'gb-motorway', 'gb-trunk'
                );
        END IF;
    END
$$;

DO
$$
    BEGIN
        BEGIN
            ALTER TABLE osm_route_member
                ADD COLUMN network_type route_network_type;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column network_type already exists in network_type.';
        END;
    END;
$$;

-- Layer transportation_name - ./update_route_member.sql

CREATE TABLE IF NOT EXISTS ne_10m_admin_0_bg_buffer AS
SELECT ST_Buffer(geometry, 10000)
FROM ne_10m_admin_0_countries
WHERE iso_a2 = 'GB';

CREATE OR REPLACE VIEW gbr_route_members_view AS
SELECT 0,
       osm_id,
       substring(ref FROM E'^[AM][0-9AM()]+'),
       CASE WHEN highway = 'motorway' THEN 'omt-gb-motorway' ELSE 'omt-gb-trunk' END
FROM osm_highway_linestring
WHERE length(ref) > 0
  AND ST_Intersects(geometry, (SELECT * FROM ne_10m_admin_0_bg_buffer))
  AND highway IN ('motorway', 'trunk')
;
-- Create GBR relations (so we can use it in the same way as other relations)
DELETE
FROM osm_route_member
WHERE network IN ('omt-gb-motorway', 'omt-gb-trunk');
-- etldoc:  osm_highway_linestring ->  osm_route_member
INSERT INTO osm_route_member (osm_id, member, ref, network)
SELECT *
FROM gbr_route_members_view;

CREATE OR REPLACE FUNCTION osm_route_member_network_type(network text, name text, ref text) RETURNS route_network_type AS
$$
SELECT CASE
           WHEN network = 'US:I' THEN 'us-interstate'::route_network_type
           WHEN network = 'US:US' THEN 'us-highway'::route_network_type
           WHEN network LIKE 'US:__' THEN 'us-state'::route_network_type
           -- https://en.wikipedia.org/wiki/Trans-Canada_Highway
           -- TODO: improve hierarchical queries using
           --    http://www.openstreetmap.org/relation/1307243
           --    however the relation does not cover the whole Trans-Canada_Highway
           WHEN
                   (network = 'CA:transcanada') OR
                   (network = 'CA:BC:primary' AND ref IN ('16')) OR
                   (name = 'Yellowhead Highway (AB)' AND ref IN ('16')) OR
                   (network = 'CA:SK:primary' AND ref IN ('16')) OR
                   (network = 'CA:ON:primary' AND ref IN ('17', '417')) OR
                   (name = 'Route Transcanadienne') OR
                   (network = 'CA:NB:primary' AND ref IN ('2', '16')) OR
                   (network = 'CA:PE' AND ref IN ('1')) OR
                   (network = 'CA:NS' AND ref IN ('104', '105')) OR
                   (network = 'CA:NL:R' AND ref IN ('1')) OR
                   (name = 'Trans-Canada Highway')
               THEN 'ca-transcanada'::route_network_type
           WHEN network = 'omt-gb-motorway' THEN 'gb-motorway'::route_network_type
           WHEN network = 'omt-gb-trunk' THEN 'gb-trunk'::route_network_type
           END;
$$ LANGUAGE sql IMMUTABLE
                PARALLEL SAFE;

-- etldoc:  osm_route_member ->  osm_route_member
-- see http://wiki.openstreetmap.org/wiki/Relation:route#Road_routes
UPDATE osm_route_member
SET network_type = osm_route_member_network_type(network, name, ref)
WHERE network != ''
  AND network_type != osm_route_member_network_type(network, name, ref)
;

CREATE OR REPLACE FUNCTION update_osm_route_member() RETURNS void AS
$$
BEGIN
    DELETE
    FROM osm_route_member AS r
        USING
            transportation_name.network_changes AS c
    WHERE network IN ('omt-gb-motorway', 'omt-gb-trunk')
      AND r.osm_id = c.osm_id;

    INSERT INTO osm_route_member (osm_id, member, ref, network)
    SELECT r.*
    FROM gbr_route_members_view AS r
             JOIN transportation_name.network_changes AS c ON
        r.osm_id = c.osm_id;

    UPDATE
        osm_route_member AS r
    SET network_type = osm_route_member_network_type(network, name, ref)
    FROM transportation_name.network_changes AS c
    WHERE network != ''
      AND network_type != osm_route_member_network_type(network, name, ref)
      AND r.member = c.osm_id;
END;
$$ LANGUAGE plpgsql;

CREATE INDEX IF NOT EXISTS osm_route_member_network_idx ON osm_route_member ("network");
CREATE INDEX IF NOT EXISTS osm_route_member_member_idx ON osm_route_member ("member");
CREATE INDEX IF NOT EXISTS osm_route_member_name_idx ON osm_route_member ("name");
CREATE INDEX IF NOT EXISTS osm_route_member_ref_idx ON osm_route_member ("ref");

CREATE INDEX IF NOT EXISTS osm_route_member_network_type_idx ON osm_route_member ("network_type");

-- Layer transportation_name - ./update_transportation_name.sql

-- Instead of using relations to find out the road names we
-- stitch together the touching ways with the same name
-- to allow for nice label rendering
-- Because this works well for roads that do not have relations as well


-- etldoc: osm_highway_linestring ->  osm_transportation_name_network
-- etldoc: osm_route_member ->  osm_transportation_name_network
CREATE TABLE IF NOT EXISTS osm_transportation_name_network AS
SELECT
    geometry,
    osm_id,
    name,
    name_en,
    name_de,
    tags,
    ref,
    highway,
    construction,
    brunnel,
    "level",
    layer,
    indoor,
    network_type,
    z_order
FROM (
    SELECT hl.geometry,
        hl.osm_id,
        CASE WHEN length(hl.name) > 15 THEN osml10n_street_abbrev_all(hl.name) ELSE NULLIF(hl.name, '') END AS "name",
        CASE WHEN length(hl.name_en) > 15 THEN osml10n_street_abbrev_en(hl.name_en) ELSE NULLIF(hl.name_en, '') END AS "name_en",
        CASE WHEN length(hl.name_de) > 15 THEN osml10n_street_abbrev_de(hl.name_de) ELSE NULLIF(hl.name_de, '') END AS "name_de",
        slice_language_tags(hl.tags) AS tags,
        rm.network_type,
        CASE
            WHEN rm.network_type IS NOT NULL AND nullif(rm.ref::text, '') IS NOT NULL
                THEN rm.ref::text
            ELSE NULLIF(hl.ref, '')
            END AS ref,
        hl.highway,
        hl.construction,
        brunnel(hl.is_bridge, hl.is_tunnel, hl.is_ford) AS brunnel,
        CASE WHEN highway IN ('footway', 'steps') THEN layer END AS layer,
        CASE WHEN highway IN ('footway', 'steps') THEN level END AS level,
        CASE WHEN highway IN ('footway', 'steps') THEN indoor END AS indoor,
        ROW_NUMBER() OVER (PARTITION BY hl.osm_id
            ORDER BY rm.network_type) AS "rank",
        hl.z_order
    FROM osm_highway_linestring hl
            LEFT JOIN osm_route_member rm ON
        rm.member = hl.osm_id
    WHERE (hl.name <> '' OR hl.ref <> '')
      AND NULLIF(hl.highway, '') IS NOT NULL
) AS t
WHERE ("rank" = 1 OR "rank" IS NULL);
CREATE INDEX IF NOT EXISTS osm_transportation_name_network_osm_id_idx ON osm_transportation_name_network (osm_id);
CREATE INDEX IF NOT EXISTS osm_transportation_name_network_name_ref_idx ON osm_transportation_name_network (coalesce(name, ''), coalesce(ref, ''));
CREATE INDEX IF NOT EXISTS osm_transportation_name_network_geometry_idx ON osm_transportation_name_network USING gist (geometry);


-- etldoc: osm_transportation_name_network ->  osm_transportation_name_linestring
CREATE TABLE IF NOT EXISTS osm_transportation_name_linestring AS
SELECT (ST_Dump(geometry)).geom AS geometry,
       NULL::bigint AS osm_id,
       name,
       name_en,
       name_de,
       tags || get_basic_names(tags, geometry) AS "tags",
       ref,
       highway,
       construction,
       brunnel,
       "level",
       layer,
       indoor,
       network_type AS network,
       z_order
FROM (
         SELECT ST_LineMerge(ST_Collect(geometry)) AS geometry,
                name,
                name_en,
                name_de,
                tags || hstore( -- store results of osml10n_street_abbrev_* above
                               ARRAY ['name', name, 'name:en', name_en, 'name:de', name_de]) AS tags,
                ref,
                highway,
                construction,
                brunnel,
                "level",
                layer,
                indoor,
                network_type,
                min(z_order) AS z_order
         FROM osm_transportation_name_network
         GROUP BY name, name_en, name_de, tags, ref, highway, construction, brunnel, "level", layer, indoor, network_type
     ) AS highway_union
;
CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_name_ref_idx ON osm_transportation_name_linestring (coalesce(name, ''), coalesce(ref, ''));
CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_geometry_idx ON osm_transportation_name_linestring USING gist (geometry);

CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_highway_partial_idx
    ON osm_transportation_name_linestring (highway, construction)
    WHERE highway IN ('motorway', 'trunk', 'construction');

-- etldoc: osm_transportation_name_linestring -> osm_transportation_name_linestring_gen1
CREATE OR REPLACE VIEW osm_transportation_name_linestring_gen1_view AS
SELECT ST_Simplify(geometry, 50) AS geometry,
       osm_id,
       name,
       name_en,
       name_de,
       tags,
       ref,
       highway,
       construction,
       brunnel,
       network,
       z_order
FROM osm_transportation_name_linestring
WHERE (highway IN ('motorway', 'trunk') OR highway = 'construction' AND construction IN ('motorway', 'trunk'))
  AND ST_Length(geometry) > 8000
;
CREATE TABLE IF NOT EXISTS osm_transportation_name_linestring_gen1 AS
SELECT *
FROM osm_transportation_name_linestring_gen1_view;
CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_gen1_name_ref_idx ON osm_transportation_name_linestring_gen1((coalesce(name, ref)));
CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_gen1_geometry_idx ON osm_transportation_name_linestring_gen1 USING gist (geometry);

CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_gen1_highway_partial_idx
    ON osm_transportation_name_linestring_gen1 (highway, construction)
    WHERE highway IN ('motorway', 'trunk', 'construction');

-- etldoc: osm_transportation_name_linestring_gen1 -> osm_transportation_name_linestring_gen2
CREATE OR REPLACE VIEW osm_transportation_name_linestring_gen2_view AS
SELECT ST_Simplify(geometry, 120) AS geometry,
       osm_id,
       name,
       name_en,
       name_de,
       tags,
       ref,
       highway,
       construction,
       brunnel,
       network,
       z_order
FROM osm_transportation_name_linestring_gen1
WHERE (highway IN ('motorway', 'trunk') OR highway = 'construction' AND construction IN ('motorway', 'trunk'))
  AND ST_Length(geometry) > 14000
;
CREATE TABLE IF NOT EXISTS osm_transportation_name_linestring_gen2 AS
SELECT *
FROM osm_transportation_name_linestring_gen2_view;
CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_gen2_name_ref_idx ON osm_transportation_name_linestring_gen2((coalesce(name, ref)));
CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_gen2_geometry_idx ON osm_transportation_name_linestring_gen2 USING gist (geometry);

CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_gen2_highway_partial_idx
    ON osm_transportation_name_linestring_gen2 (highway, construction)
    WHERE highway IN ('motorway', 'trunk', 'construction');

-- etldoc: osm_transportation_name_linestring_gen2 -> osm_transportation_name_linestring_gen3
CREATE OR REPLACE VIEW osm_transportation_name_linestring_gen3_view AS
SELECT ST_Simplify(geometry, 200) AS geometry,
       osm_id,
       name,
       name_en,
       name_de,
       tags,
       ref,
       highway,
       construction,
       brunnel,
       network,
       z_order
FROM osm_transportation_name_linestring_gen2
WHERE (highway = 'motorway' OR highway = 'construction' AND construction = 'motorway')
  AND ST_Length(geometry) > 20000
;
CREATE TABLE IF NOT EXISTS osm_transportation_name_linestring_gen3 AS
SELECT *
FROM osm_transportation_name_linestring_gen3_view;
CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_gen3_name_ref_idx ON osm_transportation_name_linestring_gen3((coalesce(name, ref)));
CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_gen3_geometry_idx ON osm_transportation_name_linestring_gen3 USING gist (geometry);

CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_gen3_highway_partial_idx
    ON osm_transportation_name_linestring_gen3 (highway, construction)
    WHERE highway IN ('motorway', 'construction');

-- etldoc: osm_transportation_name_linestring_gen3 -> osm_transportation_name_linestring_gen4
CREATE OR REPLACE VIEW osm_transportation_name_linestring_gen4_view AS
SELECT ST_Simplify(geometry, 500) AS geometry,
       osm_id,
       name,
       name_en,
       name_de,
       tags,
       ref,
       highway,
       construction,
       brunnel,
       network,
       z_order
FROM osm_transportation_name_linestring_gen3
WHERE (highway = 'motorway' OR highway = 'construction' AND construction = 'motorway')
  AND ST_Length(geometry) > 20000
;
CREATE TABLE IF NOT EXISTS osm_transportation_name_linestring_gen4 AS
SELECT *
FROM osm_transportation_name_linestring_gen4_view;
CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_gen4_name_ref_idx ON osm_transportation_name_linestring_gen4((coalesce(name, ref)));
CREATE INDEX IF NOT EXISTS osm_transportation_name_linestring_gen4_geometry_idx ON osm_transportation_name_linestring_gen4 USING gist (geometry);

-- Handle updates

CREATE SCHEMA IF NOT EXISTS transportation_name;

-- Trigger to update "osm_transportation_name_network" from "osm_route_member" and "osm_highway_linestring"

CREATE TABLE IF NOT EXISTS transportation_name.network_changes
(
    osm_id bigint,
    UNIQUE (osm_id)
);

CREATE OR REPLACE FUNCTION transportation_name.route_member_store() RETURNS trigger AS
$$
BEGIN
    INSERT INTO transportation_name.network_changes(osm_id)
    VALUES (CASE WHEN tg_op IN ('DELETE', 'UPDATE') THEN old.member ELSE new.member END)
    ON CONFLICT(osm_id) DO NOTHING;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION transportation_name.highway_linestring_store() RETURNS trigger AS
$$
BEGIN
    INSERT INTO transportation_name.network_changes(osm_id)
    VALUES (CASE WHEN tg_op IN ('DELETE', 'UPDATE') THEN old.osm_id ELSE new.osm_id END)
    ON CONFLICT(osm_id) DO NOTHING;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS transportation_name.updates_network
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION transportation_name.flag_network() RETURNS trigger AS
$$
BEGIN
    INSERT INTO transportation_name.updates_network(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION transportation_name.refresh_network() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh transportation_name_network';
    PERFORM update_osm_route_member();

    -- REFRESH osm_transportation_name_network
    DELETE
    FROM osm_transportation_name_network AS n
        USING
            transportation_name.network_changes AS c
    WHERE n.osm_id = c.osm_id;

    INSERT INTO osm_transportation_name_network
    SELECT
        geometry,
        osm_id,
        name,
        name_en,
        name_de,
        tags,
        ref,
        highway,
        construction,
        brunnel,
        level,
        layer,
        indoor,
        network_type,
        z_order
    FROM (
        SELECT hl.geometry,
            hl.osm_id,
            CASE WHEN length(hl.name) > 15 THEN osml10n_street_abbrev_all(hl.name) ELSE NULLIF(hl.name, '') END AS name,
            CASE WHEN length(hl.name_en) > 15 THEN osml10n_street_abbrev_en(hl.name_en) ELSE NULLIF(hl.name_en, '') END AS name_en,
            CASE WHEN length(hl.name_de) > 15 THEN osml10n_street_abbrev_de(hl.name_de) ELSE NULLIF(hl.name_de, '') END AS name_de,
            slice_language_tags(hl.tags) AS tags,
            rm.network_type,
            CASE
                WHEN rm.network_type IS NOT NULL AND NULLIF(rm.ref::text, '') IS NOT NULL
                    THEN rm.ref::text
                ELSE NULLIF(hl.ref, '')
                END AS ref,
            hl.highway,
            hl.construction,
            brunnel(hl.is_bridge, hl.is_tunnel, hl.is_ford) AS brunnel,
            CASE WHEN highway IN ('footway', 'steps') THEN layer END AS layer,
            CASE WHEN highway IN ('footway', 'steps') THEN level END AS level,
            CASE WHEN highway IN ('footway', 'steps') THEN indoor END AS indoor,
            ROW_NUMBER() OVER (PARTITION BY hl.osm_id
                ORDER BY rm.network_type) AS "rank",
            hl.z_order
        FROM osm_highway_linestring hl
                JOIN transportation_name.network_changes AS c ON
            hl.osm_id = c.osm_id
                LEFT JOIN osm_route_member rm ON
            rm.member = hl.osm_id
        WHERE (hl.name <> '' OR hl.ref <> '')
          AND NULLIF(hl.highway, '') IS NOT NULL
    ) AS t
    WHERE ("rank" = 1 OR "rank" IS NULL);

    -- noinspection SqlWithoutWhere
    DELETE FROM transportation_name.network_changes;
    -- noinspection SqlWithoutWhere
    DELETE FROM transportation_name.updates_network;

    RAISE LOG 'Refresh transportation_name network done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER trigger_store_transportation_route_member
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_route_member
    FOR EACH ROW
EXECUTE PROCEDURE transportation_name.route_member_store();

CREATE TRIGGER trigger_store_transportation_highway_linestring
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_highway_linestring
    FOR EACH ROW
EXECUTE PROCEDURE transportation_name.highway_linestring_store();

CREATE TRIGGER trigger_flag_transportation_name
    AFTER INSERT
    ON transportation_name.network_changes
    FOR EACH STATEMENT
EXECUTE PROCEDURE transportation_name.flag_network();

CREATE CONSTRAINT TRIGGER trigger_refresh_network
    AFTER INSERT
    ON transportation_name.updates_network
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE transportation_name.refresh_network();

-- Trigger to update "osm_transportation_name_linestring" from "osm_transportation_name_network"

CREATE TABLE IF NOT EXISTS transportation_name.name_changes
(
    id serial PRIMARY KEY,
    is_old boolean,
    osm_id bigint,
    name character varying,
    name_en character varying,
    name_de character varying,
    ref character varying,
    highway character varying,
    construction character varying,
    brunnel character varying,
    level integer,
    layer integer,
    indoor boolean,
    network_type route_network_type
);

CREATE OR REPLACE FUNCTION transportation_name.name_network_store() RETURNS trigger AS
$$
BEGIN
    IF (tg_op IN ('DELETE', 'UPDATE'))
    THEN
        INSERT INTO transportation_name.name_changes(is_old, osm_id, name, name_en, name_de, ref, highway, construction,
                                                     brunnel, level, layer, indoor, network_type)
        VALUES (TRUE, old.osm_id, old.name, old.name_en, old.name_de, old.ref, old.highway, old.construction,
                old.brunnel, old.level, old.layer, old.indoor, old.network_type);
    END IF;
    IF (tg_op IN ('UPDATE', 'INSERT'))
    THEN
        INSERT INTO transportation_name.name_changes(is_old, osm_id, name, name_en, name_de, ref, highway, construction,
                                                     brunnel, level, layer, indoor, network_type)
        VALUES (FALSE, new.osm_id, new.name, new.name_en, new.name_de, new.ref, new.highway, new.construction,
                new.brunnel, new.level, new.layer, new.indoor, new.network_type);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS transportation_name.updates_name
(
    id serial PRIMARY KEY,
    t  text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION transportation_name.flag_name() RETURNS trigger AS
$$
BEGIN
    INSERT INTO transportation_name.updates_name(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION transportation_name.refresh_name() RETURNS trigger AS
$BODY$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh transportation_name';

    -- REFRESH osm_transportation_name_linestring

    -- Compact the change history to keep only the first and last version, and then uniq version of row
    CREATE TEMP TABLE name_changes_compact AS
    SELECT DISTINCT ON (name, name_en, name_de, ref, highway, construction, brunnel, level, layer, indoor, network_type)
        name,
        name_en,
        name_de,
        ref,
        highway,
        construction,
        brunnel,
        level,
        layer,
        indoor,
        network_type,
        coalesce(name, ref) AS name_ref
    FROM ((
              SELECT DISTINCT ON (osm_id) *
              FROM transportation_name.name_changes
              WHERE is_old
              ORDER BY osm_id,
                       id ASC
          )
          UNION ALL
          (
              SELECT DISTINCT ON (osm_id) *
              FROM transportation_name.name_changes
              WHERE NOT is_old
              ORDER BY osm_id,
                       id DESC
          )) AS t;

    DELETE
    FROM osm_transportation_name_linestring AS n
        USING name_changes_compact AS c
    WHERE coalesce(n.name, '') = coalesce(c.name, '')
      AND coalesce(n.ref, '') = coalesce(c.ref, '')
      AND n.name_en IS NOT DISTINCT FROM c.name_en
      AND n.name_de IS NOT DISTINCT FROM c.name_de
      AND n.highway IS NOT DISTINCT FROM c.highway
      AND n.construction IS NOT DISTINCT FROM c.construction
      AND n.brunnel IS NOT DISTINCT FROM c.brunnel
      AND n.level IS NOT DISTINCT FROM c.level
      AND n.layer IS NOT DISTINCT FROM c.layer
      AND n.indoor IS NOT DISTINCT FROM c.indoor
      AND n.network IS NOT DISTINCT FROM c.network_type;

    INSERT INTO osm_transportation_name_linestring
    SELECT (ST_Dump(geometry)).geom AS geometry,
           NULL::bigint AS osm_id,
           name,
           name_en,
           name_de,
           tags || get_basic_names(tags, geometry) AS tags,
           ref,
           highway,
           construction,
           brunnel,
           level,
           layer,
           indoor,
           network_type AS network,
           z_order
    FROM (
        SELECT ST_LineMerge(ST_Collect(n.geometry)) AS geometry,
            n.name,
            n.name_en,
            n.name_de,
            hstore(string_agg(nullif(slice_language_tags(tags ||
                                                         hstore(ARRAY ['name', n.name, 'name:en', n.name_en, 'name:de', n.name_de]))::text,
                                     ''), ',')) AS tags,
            n.ref,
            n.highway,
            n.construction,
            n.brunnel,
            n.level,
            n.layer,
            n.indoor,
            n.network_type,
            min(n.z_order) AS z_order
        FROM osm_transportation_name_network AS n
            JOIN name_changes_compact AS c ON
                 coalesce(n.name, '') = coalesce(c.name, '')
             AND coalesce(n.ref, '') = coalesce(c.ref, '')
             AND n.name_en IS NOT DISTINCT FROM c.name_en
             AND n.name_de IS NOT DISTINCT FROM c.name_de
             AND n.highway IS NOT DISTINCT FROM c.highway
             AND n.construction IS NOT DISTINCT FROM c.construction
             AND n.brunnel IS NOT DISTINCT FROM c.brunnel
             AND n.level IS NOT DISTINCT FROM c.level
             AND n.layer IS NOT DISTINCT FROM c.layer
             AND n.indoor IS NOT DISTINCT FROM c.indoor
             AND n.network_type IS NOT DISTINCT FROM c.network_type
        GROUP BY n.name, n.name_en, n.name_de, n.ref, n.highway, n.construction, n.brunnel, n.level, n.layer, n.indoor, n.network_type
    ) AS highway_union;

    -- REFRESH osm_transportation_name_linestring_gen1
    DELETE FROM osm_transportation_name_linestring_gen1 AS n
    USING name_changes_compact AS c
    WHERE
        coalesce(n.name, n.ref) = c.name_ref
        AND n.name IS NOT DISTINCT FROM c.name
        AND n.name_en IS NOT DISTINCT FROM c.name_en
        AND n.name_de IS NOT DISTINCT FROM c.name_de
        AND n.ref IS NOT DISTINCT FROM c.ref
        AND n.highway IS NOT DISTINCT FROM c.highway
        AND n.construction IS NOT DISTINCT FROM c.construction
        AND n.brunnel IS NOT DISTINCT FROM c.brunnel
        AND n.network IS NOT DISTINCT FROM c.network_type;

    INSERT INTO osm_transportation_name_linestring_gen1
    SELECT n.*
    FROM osm_transportation_name_linestring_gen1_view AS n
        JOIN name_changes_compact AS c ON
            coalesce(n.name, n.ref) = c.name_ref
            AND n.name IS NOT DISTINCT FROM c.name
            AND n.name_en IS NOT DISTINCT FROM c.name_en
            AND n.name_de IS NOT DISTINCT FROM c.name_de
            AND n.ref IS NOT DISTINCT FROM c.ref
            AND n.highway IS NOT DISTINCT FROM c.highway
            AND n.construction IS NOT DISTINCT FROM c.construction
            AND n.brunnel IS NOT DISTINCT FROM c.brunnel
            AND n.network IS NOT DISTINCT FROM c.network_type;

    -- REFRESH osm_transportation_name_linestring_gen2
    DELETE FROM osm_transportation_name_linestring_gen2 AS n
    USING name_changes_compact AS c
    WHERE
        coalesce(n.name, n.ref) = c.name_ref
        AND n.name IS NOT DISTINCT FROM c.name
        AND n.name_en IS NOT DISTINCT FROM c.name_en
        AND n.name_de IS NOT DISTINCT FROM c.name_de
        AND n.ref IS NOT DISTINCT FROM c.ref
        AND n.highway IS NOT DISTINCT FROM c.highway
        AND n.construction IS NOT DISTINCT FROM c.construction
        AND n.brunnel IS NOT DISTINCT FROM c.brunnel
        AND n.network IS NOT DISTINCT FROM c.network_type;

    INSERT INTO osm_transportation_name_linestring_gen2
    SELECT n.*
    FROM osm_transportation_name_linestring_gen2_view AS n
        JOIN name_changes_compact AS c ON
            coalesce(n.name, n.ref) = c.name_ref
            AND n.name IS NOT DISTINCT FROM c.name
            AND n.name_en IS NOT DISTINCT FROM c.name_en
            AND n.name_de IS NOT DISTINCT FROM c.name_de
            AND n.ref IS NOT DISTINCT FROM c.ref
            AND n.highway IS NOT DISTINCT FROM c.highway
            AND n.construction IS NOT DISTINCT FROM c.construction
            AND n.brunnel IS NOT DISTINCT FROM c.brunnel
            AND n.network IS NOT DISTINCT FROM c.network_type;

    -- REFRESH osm_transportation_name_linestring_gen3
    DELETE FROM osm_transportation_name_linestring_gen3 AS n
    USING name_changes_compact AS c
    WHERE
        coalesce(n.name, n.ref) = c.name_ref
        AND n.name IS NOT DISTINCT FROM c.name
        AND n.name_en IS NOT DISTINCT FROM c.name_en
        AND n.name_de IS NOT DISTINCT FROM c.name_de
        AND n.ref IS NOT DISTINCT FROM c.ref
        AND n.highway IS NOT DISTINCT FROM c.highway
        AND n.construction IS NOT DISTINCT FROM c.construction
        AND n.brunnel IS NOT DISTINCT FROM c.brunnel
        AND n.network IS NOT DISTINCT FROM c.network_type;

    INSERT INTO osm_transportation_name_linestring_gen3
    SELECT n.*
    FROM osm_transportation_name_linestring_gen3_view AS n
        JOIN name_changes_compact AS c ON
            coalesce(n.name, n.ref) = c.name_ref
            AND n.name IS NOT DISTINCT FROM c.name
            AND n.name_en IS NOT DISTINCT FROM c.name_en
            AND n.name_de IS NOT DISTINCT FROM c.name_de
            AND n.ref IS NOT DISTINCT FROM c.ref
            AND n.highway IS NOT DISTINCT FROM c.highway
            AND n.construction IS NOT DISTINCT FROM c.construction
            AND n.brunnel IS NOT DISTINCT FROM c.brunnel
            AND n.network IS NOT DISTINCT FROM c.network_type;

    -- REFRESH osm_transportation_name_linestring_gen4
    DELETE FROM osm_transportation_name_linestring_gen4 AS n
    USING name_changes_compact AS c
    WHERE
        coalesce(n.name, n.ref) = c.name_ref
        AND n.name IS NOT DISTINCT FROM c.name
        AND n.name_en IS NOT DISTINCT FROM c.name_en
        AND n.name_de IS NOT DISTINCT FROM c.name_de
        AND n.ref IS NOT DISTINCT FROM c.ref
        AND n.highway IS NOT DISTINCT FROM c.highway
        AND n.construction IS NOT DISTINCT FROM c.construction
        AND n.brunnel IS NOT DISTINCT FROM c.brunnel
        AND n.network IS NOT DISTINCT FROM c.network_type;

    INSERT INTO osm_transportation_name_linestring_gen4
    SELECT n.*
    FROM osm_transportation_name_linestring_gen4_view AS n
        JOIN name_changes_compact AS c ON
            coalesce(n.name, n.ref) = c.name_ref
            AND n.name IS NOT DISTINCT FROM c.name
            AND n.name_en IS NOT DISTINCT FROM c.name_en
            AND n.name_de IS NOT DISTINCT FROM c.name_de
            AND n.ref IS NOT DISTINCT FROM c.ref
            AND n.highway IS NOT DISTINCT FROM c.highway
            AND n.construction IS NOT DISTINCT FROM c.construction
            AND n.brunnel IS NOT DISTINCT FROM c.brunnel
            AND n.network IS NOT DISTINCT FROM c.network_type;

    DROP TABLE name_changes_compact;
    DELETE FROM transportation_name.name_changes;
    DELETE FROM transportation_name.updates_name;

    RAISE LOG 'Refresh transportation_name done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$BODY$
    LANGUAGE plpgsql;


CREATE TRIGGER trigger_store_transportation_name_network
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_transportation_name_network
    FOR EACH ROW
EXECUTE PROCEDURE transportation_name.name_network_store();

CREATE TRIGGER trigger_flag_name
    AFTER INSERT
    ON transportation_name.name_changes
    FOR EACH STATEMENT
EXECUTE PROCEDURE transportation_name.flag_name();

CREATE CONSTRAINT TRIGGER trigger_refresh_name
    AFTER INSERT
    ON transportation_name.updates_name
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE transportation_name.refresh_name();

-- Layer transportation_name - ./transportation_name.sql

-- etldoc: layer_transportation_name[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="layer_transportation_name | <z6> z6 | <z7> z7 | <z8> z8 |<z9> z9 |<z10> z10 |<z11> z11 |<z12> z12|<z13> z13|<z14_> z14+" ] ;

CREATE OR REPLACE FUNCTION layer_transportation_name(bbox geometry, zoom_level integer)
    RETURNS TABLE
            (
                osm_id     bigint,
                geometry   geometry,
                name       text,
                name_en    text,
                name_de    text,
                tags       hstore,
                ref        text,
                ref_length int,
                network    text,
                class      text,
                subclass   text,
                brunnel    text,
                layer      int,
                level      int,
                indoor     int
            )
AS
$$
SELECT osm_id,
       geometry,
       name,
       COALESCE(name_en, name) AS name_en,
       COALESCE(name_de, name, name_en) AS name_de,
       tags,
       ref,
       NULLIF(LENGTH(ref), 0) AS ref_length,
       --TODO: The road network of the road is not yet implemented
       CASE
           WHEN network IS NOT NULL
               THEN network::text
           WHEN length(coalesce(ref, '')) > 0
               THEN 'road'
           END AS network,
       highway_class(highway, '', construction) AS class,
       CASE
           WHEN highway IS NOT NULL AND highway_class(highway, '', construction) = 'path'
               THEN highway
           END AS subclass,
       brunnel,
       NULLIF(layer, 0) AS layer,
       "level",
       CASE WHEN indoor = TRUE THEN 1 END AS indoor
FROM (

         -- etldoc: osm_transportation_name_linestring_gen4 ->  layer_transportation_name:z6
         SELECT *,
                NULL::int AS layer,
                NULL::int AS level,
                NULL::boolean AS indoor
         FROM osm_transportation_name_linestring_gen4
         WHERE zoom_level = 6
         UNION ALL

         -- etldoc: osm_transportation_name_linestring_gen3 ->  layer_transportation_name:z7
         SELECT *,
                NULL::int AS layer,
                NULL::int AS level,
                NULL::boolean AS indoor
         FROM osm_transportation_name_linestring_gen3
         WHERE zoom_level = 7
         UNION ALL

         -- etldoc: osm_transportation_name_linestring_gen2 ->  layer_transportation_name:z8
         SELECT *,
                NULL::int AS layer,
                NULL::int AS level,
                NULL::boolean AS indoor
         FROM osm_transportation_name_linestring_gen2
         WHERE zoom_level = 8
         UNION ALL

         -- etldoc: osm_transportation_name_linestring_gen1 ->  layer_transportation_name:z9
         -- etldoc: osm_transportation_name_linestring_gen1 ->  layer_transportation_name:z10
         -- etldoc: osm_transportation_name_linestring_gen1 ->  layer_transportation_name:z11
         SELECT *,
                NULL::int AS layer,
                NULL::int AS level,
                NULL::boolean AS indoor
         FROM osm_transportation_name_linestring_gen1
         WHERE zoom_level BETWEEN 9 AND 11
         UNION ALL

         -- etldoc: osm_transportation_name_linestring ->  layer_transportation_name:z12
         SELECT geometry,
                osm_id,
                name,
                name_en,
                name_de,
                "tags",
                ref,
                highway,
                construction,
                brunnel,
                network,
                z_order,
                layer,
                "level",
                indoor
         FROM osm_transportation_name_linestring
         WHERE zoom_level = 12
           AND LineLabel(zoom_level, COALESCE(name, ref), geometry)
           AND highway_class(highway, '', construction) NOT IN ('minor', 'track', 'path')
           AND NOT highway_is_link(highway)
         UNION ALL

         -- etldoc: osm_transportation_name_linestring ->  layer_transportation_name:z13
         SELECT geometry,
                osm_id,
                name,
                name_en,
                name_de,
                "tags",
                ref,
                highway,
                construction,
                brunnel,
                network,
                z_order,
                layer,
                "level",
                indoor
         FROM osm_transportation_name_linestring
         WHERE zoom_level = 13
           AND LineLabel(zoom_level, COALESCE(name, ref), geometry)
           AND highway_class(highway, '', construction) NOT IN ('track', 'path')
         UNION ALL

         -- etldoc: osm_transportation_name_linestring ->  layer_transportation_name:z14_
         SELECT geometry,
                osm_id,
                name,
                name_en,
                name_de,
                "tags",
                ref,
                highway,
                construction,
                brunnel,
                network,
                z_order,
                layer,
                "level",
                indoor
         FROM osm_transportation_name_linestring
         WHERE zoom_level >= 14
     ) AS zoom_levels
WHERE geometry && bbox
ORDER BY z_order ASC;
$$ LANGUAGE SQL STABLE
                -- STRICT
                PARALLEL SAFE;

DO $$ BEGIN RAISE NOTICE 'Finished layer transportation_name'; END$$;
