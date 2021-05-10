DO $$ BEGIN RAISE NOTICE 'Processing layer park'; END$$;

-- Layer park - ./update_park_polygon.sql

ALTER TABLE osm_park_polygon
    ADD COLUMN IF NOT EXISTS geometry_point geometry;
ALTER TABLE osm_park_polygon_gen_z13
    ADD COLUMN IF NOT EXISTS geometry_point geometry;
ALTER TABLE osm_park_polygon_gen_z12
    ADD COLUMN IF NOT EXISTS geometry_point geometry;
ALTER TABLE osm_park_polygon_gen_z11
    ADD COLUMN IF NOT EXISTS geometry_point geometry;
ALTER TABLE osm_park_polygon_gen_z10
    ADD COLUMN IF NOT EXISTS geometry_point geometry;
ALTER TABLE osm_park_polygon_gen_z9
    ADD COLUMN IF NOT EXISTS geometry_point geometry;
ALTER TABLE osm_park_polygon_gen_z8
    ADD COLUMN IF NOT EXISTS geometry_point geometry;
ALTER TABLE osm_park_polygon_gen_z7
    ADD COLUMN IF NOT EXISTS geometry_point geometry;
ALTER TABLE osm_park_polygon_gen_z6
    ADD COLUMN IF NOT EXISTS geometry_point geometry;

DROP TRIGGER IF EXISTS update_row ON osm_park_polygon;
DROP TRIGGER IF EXISTS update_row ON osm_park_polygon_gen_z13;
DROP TRIGGER IF EXISTS update_row ON osm_park_polygon_gen_z12;
DROP TRIGGER IF EXISTS update_row ON osm_park_polygon_gen_z11;
DROP TRIGGER IF EXISTS update_row ON osm_park_polygon_gen_z10;
DROP TRIGGER IF EXISTS update_row ON osm_park_polygon_gen_z9;
DROP TRIGGER IF EXISTS update_row ON osm_park_polygon_gen_z8;
DROP TRIGGER IF EXISTS update_row ON osm_park_polygon_gen_z7;
DROP TRIGGER IF EXISTS update_row ON osm_park_polygon_gen_z6;

-- etldoc:  osm_park_polygon ->  osm_park_polygon
-- etldoc:  osm_park_polygon_gen_z13 ->  osm_park_polygon_gen_z13
-- etldoc:  osm_park_polygon_gen_z12 ->  osm_park_polygon_gen_z12
-- etldoc:  osm_park_polygon_gen_z11 ->  osm_park_polygon_gen_z11
-- etldoc:  osm_park_polygon_gen_z10 ->  osm_park_polygon_gen_z10
-- etldoc:  osm_park_polygon_gen_z9 ->  osm_park_polygon_gen_z9
-- etldoc:  osm_park_polygon_gen_z8 ->  osm_park_polygon_gen_z8
-- etldoc:  osm_park_polygon_gen_z7 ->  osm_park_polygon_gen_z7
-- etldoc:  osm_park_polygon_gen_z6 ->  osm_park_polygon_gen_z6
CREATE OR REPLACE FUNCTION update_osm_park_polygon() RETURNS void AS
$$
BEGIN
    UPDATE osm_park_polygon
    SET tags           = update_tags(tags, geometry),
        geometry_point = st_centroid(geometry);

    UPDATE osm_park_polygon_gen_z13
    SET tags           = update_tags(tags, geometry),
        geometry_point = st_centroid(geometry);

    UPDATE osm_park_polygon_gen_z12
    SET tags           = update_tags(tags, geometry),
        geometry_point = st_centroid(geometry);

    UPDATE osm_park_polygon_gen_z11
    SET tags           = update_tags(tags, geometry),
        geometry_point = st_centroid(geometry);

    UPDATE osm_park_polygon_gen_z10
    SET tags           = update_tags(tags, geometry),
        geometry_point = st_centroid(geometry);

    UPDATE osm_park_polygon_gen_z9
    SET tags           = update_tags(tags, geometry),
        geometry_point = st_centroid(geometry);

    UPDATE osm_park_polygon_gen_z8
    SET tags           = update_tags(tags, geometry),
        geometry_point = st_centroid(geometry);

    UPDATE osm_park_polygon_gen_z7
    SET tags           = update_tags(tags, geometry),
        geometry_point = st_centroid(geometry);

    UPDATE osm_park_polygon_gen_z6
    SET tags           = update_tags(tags, geometry),
        geometry_point = st_centroid(geometry);

END;
$$ LANGUAGE plpgsql;

SELECT update_osm_park_polygon();
CREATE INDEX IF NOT EXISTS osm_park_polygon_point_geom_idx ON osm_park_polygon USING gist (geometry_point);
CREATE INDEX IF NOT EXISTS osm_park_polygon_gen_z13_point_geom_idx ON osm_park_polygon_gen_z13 USING gist (geometry_point);
CREATE INDEX IF NOT EXISTS osm_park_polygon_gen_z12_point_geom_idx ON osm_park_polygon_gen_z12 USING gist (geometry_point);
CREATE INDEX IF NOT EXISTS osm_park_polygon_gen_z11_point_geom_idx ON osm_park_polygon_gen_z11 USING gist (geometry_point);
CREATE INDEX IF NOT EXISTS osm_park_polygon_gen_z10_point_geom_idx ON osm_park_polygon_gen_z10 USING gist (geometry_point);
CREATE INDEX IF NOT EXISTS osm_park_polygon_gen_z9_point_geom_idx ON osm_park_polygon_gen_z9 USING gist (geometry_point);
CREATE INDEX IF NOT EXISTS osm_park_polygon_gen_z8_point_geom_idx ON osm_park_polygon_gen_z8 USING gist (geometry_point);
CREATE INDEX IF NOT EXISTS osm_park_polygon_gen_z7_point_geom_idx ON osm_park_polygon_gen_z7 USING gist (geometry_point);
CREATE INDEX IF NOT EXISTS osm_park_polygon_gen_z6_point_geom_idx ON osm_park_polygon_gen_z6 USING gist (geometry_point);


CREATE OR REPLACE FUNCTION update_osm_park_polygon_row()
    RETURNS trigger
AS
$$
BEGIN
    NEW.tags = update_tags(NEW.tags, NEW.geometry);
    NEW.geometry_point = st_centroid(NEW.geometry);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_row
    BEFORE INSERT OR UPDATE
    ON osm_park_polygon
    FOR EACH ROW
EXECUTE PROCEDURE update_osm_park_polygon_row();

CREATE TRIGGER update_row
    BEFORE INSERT OR UPDATE
    ON osm_park_polygon_gen_z13
    FOR EACH ROW
EXECUTE PROCEDURE update_osm_park_polygon_row();

CREATE TRIGGER update_row
    BEFORE INSERT OR UPDATE
    ON osm_park_polygon_gen_z12
    FOR EACH ROW
EXECUTE PROCEDURE update_osm_park_polygon_row();

CREATE TRIGGER update_row
    BEFORE INSERT OR UPDATE
    ON osm_park_polygon_gen_z11
    FOR EACH ROW
EXECUTE PROCEDURE update_osm_park_polygon_row();

CREATE TRIGGER update_row
    BEFORE INSERT OR UPDATE
    ON osm_park_polygon_gen_z10
    FOR EACH ROW
EXECUTE PROCEDURE update_osm_park_polygon_row();

CREATE TRIGGER update_row
    BEFORE INSERT OR UPDATE
    ON osm_park_polygon_gen_z9
    FOR EACH ROW
EXECUTE PROCEDURE update_osm_park_polygon_row();

CREATE TRIGGER update_row
    BEFORE INSERT OR UPDATE
    ON osm_park_polygon_gen_z8
    FOR EACH ROW
EXECUTE PROCEDURE update_osm_park_polygon_row();

CREATE TRIGGER update_row
    BEFORE INSERT OR UPDATE
    ON osm_park_polygon_gen_z7
    FOR EACH ROW
EXECUTE PROCEDURE update_osm_park_polygon_row();

CREATE TRIGGER update_row
    BEFORE INSERT OR UPDATE
    ON osm_park_polygon_gen_z6
    FOR EACH ROW
EXECUTE PROCEDURE update_osm_park_polygon_row();

-- Layer park - ./park.sql

-- etldoc: layer_park[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="layer_park |<z6> z6 |<z7> z7 |<z8> z8 |<z9> z9 |<z10> z10 |<z11> z11 |<z12> z12|<z13> z13|<z14> z14+" ] ;

CREATE OR REPLACE FUNCTION layer_park(bbox geometry, zoom_level int, pixel_width numeric)
    RETURNS TABLE
            (
                osm_id   bigint,
                geometry geometry,
                class    text,
                name     text,
                name_en  text,
                name_de  text,
                tags     hstore,
                rank     int
            )
AS
$$
SELECT osm_id,
       geometry,
       class,
       name,
       name_en,
       name_de,
       tags,
       rank
FROM (
         SELECT osm_id,
                geometry,
                COALESCE(
                        LOWER(REPLACE(NULLIF(protection_title, ''), ' ', '_')),
                        NULLIF(boundary, ''),
                        NULLIF(leisure, '')
                    ) AS class,
                name,
                name_en,
                name_de,
                tags,
                NULL::int AS rank
         FROM (
                  -- etldoc: osm_park_polygon_gen_z6 -> layer_park:z6
                  SELECT osm_id,
                         geometry,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title
                  FROM osm_park_polygon_gen_z6
                  WHERE zoom_level = 6
                    AND geometry && bbox
                  UNION ALL
                  -- etldoc: osm_park_polygon_gen_z7 -> layer_park:z7
                  SELECT osm_id,
                         geometry,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title
                  FROM osm_park_polygon_gen_z7
                  WHERE zoom_level = 7
                    AND geometry && bbox
                  UNION ALL
                  -- etldoc: osm_park_polygon_gen_z8 -> layer_park:z8
                  SELECT osm_id,
                         geometry,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title
                  FROM osm_park_polygon_gen_z8
                  WHERE zoom_level = 8
                    AND geometry && bbox
                  UNION ALL
                  -- etldoc: osm_park_polygon_gen_z9 -> layer_park:z9
                  SELECT osm_id,
                         geometry,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title
                  FROM osm_park_polygon_gen_z9
                  WHERE zoom_level = 9
                    AND geometry && bbox
                  UNION ALL
                  -- etldoc: osm_park_polygon_gen_z10 -> layer_park:z10
                  SELECT osm_id,
                         geometry,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title
                  FROM osm_park_polygon_gen_z10
                  WHERE zoom_level = 10
                    AND geometry && bbox
                  UNION ALL
                  -- etldoc: osm_park_polygon_gen_z11 -> layer_park:z11
                  SELECT osm_id,
                         geometry,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title
                  FROM osm_park_polygon_gen_z11
                  WHERE zoom_level = 11
                    AND geometry && bbox
                  UNION ALL
                  -- etldoc: osm_park_polygon_gen_z12 -> layer_park:z12
                  SELECT osm_id,
                         geometry,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title
                  FROM osm_park_polygon_gen_z12
                  WHERE zoom_level = 12
                    AND geometry && bbox
                  UNION ALL
                  -- etldoc: osm_park_polygon_gen_z13 -> layer_park:z13
                  SELECT osm_id,
                         geometry,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title
                  FROM osm_park_polygon_gen_z13
                  WHERE zoom_level = 13
                    AND geometry && bbox
                  UNION ALL
                  -- etldoc: osm_park_polygon -> layer_park:z14
                  SELECT osm_id,
                         geometry,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title
                  FROM osm_park_polygon
                  WHERE zoom_level >= 14
                    AND geometry && bbox
              ) AS park_polygon

         UNION ALL
         SELECT osm_id,
                geometry_point AS geometry,
                COALESCE(
                        LOWER(REPLACE(NULLIF(protection_title, ''), ' ', '_')),
                        NULLIF(boundary, ''),
                        NULLIF(leisure, '')
                    ) AS class,
                name,
                name_en,
                name_de,
                tags,
                row_number() OVER (
                    PARTITION BY LabelGrid(geometry_point, 100 * pixel_width)
                    ORDER BY
                        (CASE WHEN boundary = 'national_park' THEN TRUE ELSE FALSE END) DESC,
                        (COALESCE(NULLIF(tags->'wikipedia', ''), NULLIF(tags->'wikidata', '')) IS NOT NULL) DESC,
                        area DESC
                    )::int AS "rank"
         FROM (
                  -- etldoc: osm_park_polygon_gen_z6 -> layer_park:z6
                  SELECT osm_id,
                         geometry_point,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title,
                         area
                  FROM osm_park_polygon_gen_z6
                  WHERE zoom_level = 6
                    AND geometry_point && bbox
                    AND area > 70000*2^(20-zoom_level)
                  UNION ALL

                  -- etldoc: osm_park_polygon_gen_z7 -> layer_park:z7
                  SELECT osm_id,
                         geometry_point,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title,
                         area
                  FROM osm_park_polygon_gen_z7
                  WHERE zoom_level = 7
                    AND geometry_point && bbox
                    AND area > 70000*2^(20-zoom_level)
                  UNION ALL

                  -- etldoc: osm_park_polygon_gen_z8 -> layer_park:z8
                  SELECT osm_id,
                         geometry_point,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title,
                         area
                  FROM osm_park_polygon_gen_z8
                  WHERE zoom_level = 8
                    AND geometry_point && bbox
                    AND area > 70000*2^(20-zoom_level)
                  UNION ALL

                  -- etldoc: osm_park_polygon_gen_z9 -> layer_park:z9
                  SELECT osm_id,
                         geometry_point,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title,
                         area
                  FROM osm_park_polygon_gen_z9
                  WHERE zoom_level = 9
                    AND geometry_point && bbox
                    AND area > 70000*2^(20-zoom_level)
                  UNION ALL

                  -- etldoc: osm_park_polygon_gen_z10 -> layer_park:z10
                  SELECT osm_id,
                         geometry_point,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title,
                         area
                  FROM osm_park_polygon_gen_z10
                  WHERE zoom_level = 10
                    AND geometry_point && bbox
                    AND area > 70000*2^(20-zoom_level)
                  UNION ALL

                  -- etldoc: osm_park_polygon_gen_z11 -> layer_park:z11
                  SELECT osm_id,
                         geometry_point,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title,
                         area
                  FROM osm_park_polygon_gen_z11
                  WHERE zoom_level = 11
                    AND geometry_point && bbox
                    AND area > 70000*2^(20-zoom_level)
                  UNION ALL

                  -- etldoc: osm_park_polygon_gen_z12 -> layer_park:z12
                  SELECT osm_id,
                         geometry_point,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title,
                         area
                  FROM osm_park_polygon_gen_z12
                  WHERE zoom_level = 12
                    AND geometry_point && bbox
                    AND area > 70000*2^(20-zoom_level)
                  UNION ALL

                  -- etldoc: osm_park_polygon_gen_z13 -> layer_park:z13
                  SELECT osm_id,
                         geometry_point,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title,
                         area
                  FROM osm_park_polygon_gen_z13
                  WHERE zoom_level = 13
                    AND geometry_point && bbox
                    AND area > 70000*2^(20-zoom_level)
                  UNION ALL

                  -- etldoc: osm_park_polygon -> layer_park:z14
                  SELECT osm_id,
                         geometry_point,
                         name,
                         name_en,
                         name_de,
                         tags,
                         leisure,
                         boundary,
                         protection_title,
                         area
                  FROM osm_park_polygon
                  WHERE zoom_level >= 14
                    AND geometry_point && bbox
              ) AS park_point
     ) AS park_all;
$$ LANGUAGE SQL STABLE
                PARALLEL SAFE;
-- TODO: Check if the above can be made STRICT -- i.e. if pixel_width could be NULL

DO $$ BEGIN RAISE NOTICE 'Finished layer park'; END$$;
