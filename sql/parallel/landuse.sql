DO $$ BEGIN RAISE NOTICE 'Processing layer landuse'; END$$;

-- Layer landuse - ./landuse.sql

-- ne_50m_urban_areas
-- etldoc: ne_50m_urban_areas ->  ne_50m_urban_areas_gen_z5
DROP MATERIALIZED VIEW IF EXISTS ne_50m_urban_areas_gen_z5 CASCADE;
CREATE MATERIALIZED VIEW ne_50m_urban_areas_gen_z5 AS
(
SELECT
       NULL::bigint AS osm_id,
       ST_Simplify(geometry, ZRes(7)) as geometry,
       'residential'::text AS landuse,
       NULL::text AS amenity,
       NULL::text AS leisure,
       NULL::text AS tourism,
       NULL::text AS place,
       NULL::text AS waterway,
       scalerank
FROM ne_50m_urban_areas
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS ne_50m_urban_areas_gen_z5_idx ON ne_50m_urban_areas_gen_z5 USING gist (geometry);

-- etldoc: ne_50m_urban_areas_gen_z5 ->  ne_50m_urban_areas_gen_z4
DROP MATERIALIZED VIEW IF EXISTS ne_50m_urban_areas_gen_z4 CASCADE;
CREATE MATERIALIZED VIEW ne_50m_urban_areas_gen_z4 AS
(
SELECT
       osm_id,
       ST_Simplify(geometry, ZRes(6)) as geometry,
       landuse,
       amenity,
       leisure,
       tourism,
       place,
       waterway
FROM ne_50m_urban_areas_gen_z5
WHERE scalerank <= 2
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS ne_50m_urban_areas_gen_z4_idx ON ne_50m_urban_areas_gen_z4 USING gist (geometry);

-- etldoc: layer_landuse[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="layer_landuse |<z4> z4|<z5> z5|<z6> z6|<z7> z7|<z8> z8|<z9> z9|<z10> z10|<z11> z11|<z12> z12|<z13> z13|<z14> z14+" ] ;

CREATE OR REPLACE FUNCTION layer_landuse(bbox geometry, zoom_level int)
    RETURNS TABLE
            (
                osm_id   bigint,
                geometry geometry,
                class    text
            )
AS
$$
SELECT osm_id,
       geometry,
       COALESCE(
               NULLIF(landuse, ''),
               NULLIF(amenity, ''),
               NULLIF(leisure, ''),
               NULLIF(tourism, ''),
               NULLIF(place, ''),
               NULLIF(waterway, '')
           ) AS class
FROM (
         -- etldoc: ne_50m_urban_areas_gen_z4 -> layer_landuse:z4
         SELECT osm_id,
                geometry,
                landuse,
                amenity,
                leisure,
                tourism,
                place,
                waterway
         FROM ne_50m_urban_areas_gen_z4
         WHERE zoom_level = 4
         UNION ALL
         -- etldoc: ne_50m_urban_areas_gen_z5 -> layer_landuse:z5
         SELECT osm_id,
                geometry,
                landuse,
                amenity,
                leisure,
                tourism,
                place,
                waterway
         FROM ne_50m_urban_areas_gen_z5
         WHERE zoom_level = 5
         UNION ALL
         -- etldoc: osm_landuse_polygon_gen_z6 -> layer_landuse:z6
         SELECT osm_id,
                geometry,
                landuse,
                amenity,
                leisure,
                tourism,
                place,
                waterway
         FROM osm_landuse_polygon_gen_z6
         WHERE zoom_level = 6
         UNION ALL
         -- etldoc: osm_landuse_polygon_gen_z7 -> layer_landuse:z7
         SELECT osm_id,
                geometry,
                landuse,
                amenity,
                leisure,
                tourism,
                place,
                waterway
         FROM osm_landuse_polygon_gen_z7
         WHERE zoom_level = 7
         UNION ALL
         -- etldoc: osm_landuse_polygon_gen_z8 -> layer_landuse:z8
         SELECT osm_id,
                geometry,
                landuse,
                amenity,
                leisure,
                tourism,
                place,
                waterway
         FROM osm_landuse_polygon_gen_z8
         WHERE zoom_level = 8
         UNION ALL
         -- etldoc: osm_landuse_polygon_gen_z9 -> layer_landuse:z9
         SELECT osm_id,
                geometry,
                landuse,
                amenity,
                leisure,
                tourism,
                place,
                waterway
         FROM osm_landuse_polygon_gen_z9
         WHERE zoom_level = 9
         UNION ALL
         -- etldoc: osm_landuse_polygon_gen_z10 -> layer_landuse:z10
         SELECT osm_id,
                geometry,
                landuse,
                amenity,
                leisure,
                tourism,
                place,
                waterway
         FROM osm_landuse_polygon_gen_z10
         WHERE zoom_level = 10
         UNION ALL
         -- etldoc: osm_landuse_polygon_gen_z11 -> layer_landuse:z11
         SELECT osm_id,
                geometry,
                landuse,
                amenity,
                leisure,
                tourism,
                place,
                waterway
         FROM osm_landuse_polygon_gen_z11
         WHERE zoom_level = 11
         UNION ALL
         -- etldoc: osm_landuse_polygon_gen_z12 -> layer_landuse:z12
         SELECT osm_id,
                geometry,
                landuse,
                amenity,
                leisure,
                tourism,
                place,
                waterway
         FROM osm_landuse_polygon_gen_z12
         WHERE zoom_level = 12
         UNION ALL
         -- etldoc: osm_landuse_polygon_gen_z13 -> layer_landuse:z13
         SELECT osm_id,
                geometry,
                landuse,
                amenity,
                leisure,
                tourism,
                place,
                waterway
         FROM osm_landuse_polygon_gen_z13
         WHERE zoom_level = 13
         UNION ALL
         -- etldoc: osm_landuse_polygon -> layer_landuse:z14
         SELECT osm_id,
                geometry,
                landuse,
                amenity,
                leisure,
                tourism,
                place,
                waterway
         FROM osm_landuse_polygon
         WHERE zoom_level >= 14
     ) AS zoom_levels
WHERE geometry && bbox;
$$ LANGUAGE SQL STABLE
                -- STRICT
                PARALLEL SAFE;

DO $$ BEGIN RAISE NOTICE 'Finished layer landuse'; END$$;
