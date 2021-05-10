DO $$ BEGIN RAISE NOTICE 'Processing layer building'; END$$;

-- Layer building - ./update_building.sql

DROP TRIGGER IF EXISTS trigger_refresh ON buildings.updates;
DROP TRIGGER IF EXISTS trigger_flag ON osm_building_polygon;

-- Creating aggregated building blocks with removed small polygons and small
-- holes. Aggregated polygons are simplified by Visvalingam-Whyatt algorithm.
-- Aggregating is made block by block using country_osm_grid polygon table.

-- Function returning recordset for matview.
-- Returning recordset of buildings aggregates by zres 14, with removed small
-- holes and with removed small buildings/blocks.

CREATE OR REPLACE FUNCTION osm_building_block_gen1()
    RETURNS table
            (
                osm_id   bigint,
                geometry geometry
            )
AS
$$
DECLARE
    zres14 float := Zres(14);
    zres12 float := Zres(12);
    zres14vw float := Zres(14) * Zres(14);
    polyg_world record;

BEGIN
    FOR polyg_world IN 
        SELECT ST_Transform(country.geometry, 3857) AS geometry 
        FROM country_osm_grid country
        
        LOOP
            FOR osm_id, geometry IN
                WITH dta AS ( -- CTE is used because of optimization
                    SELECT o.osm_id,
                            o.geometry,
                            ST_ClusterDBSCAN(o.geometry, eps := zres14, minpoints := 1) OVER () cid
                    FROM osm_building_polygon o
                    WHERE ST_Intersects(o.geometry, polyg_world.geometry)
                )
                SELECT (array_agg(dta.osm_id))[1] AS osm_id,
                    ST_Buffer(
                        ST_Union(
                            ST_Buffer(
                                ST_SnapToGrid(dta.geometry, 0.000001)
                                , zres14, 'join=mitre')
                            )
                        , -zres14, 'join=mitre') AS geometry
                FROM dta
                GROUP BY cid

                LOOP
                    -- removing holes smaller than
                    IF ST_NumInteriorRings(geometry) > 0 THEN -- only from geometries wih holes
                        geometry := (
                            -- there are some multi-geometries in this layer
                            SELECT ST_Collect(gn)
                            FROM (
                                    -- in some cases are "holes" NULL, because all holes are smaller than
                                    SELECT COALESCE(
                                                -- exterior ring
                                                    ST_MakePolygon(ST_ExteriorRing(dmp.geom), holes),
                                                    ST_MakePolygon(ST_ExteriorRing(dmp.geom))
                                                ) gn

                                    FROM ST_Dump(geometry) dmp, -- 1 dump polygons
                                        LATERAL (
                                            SELECT array_agg(ST_Boundary(rg.geom)) holes -- 2 create array
                                            FROM ST_DumpRings(dmp.geom) rg -- 3 from rings
                                            WHERE rg.path[1] > 0 -- 5 except inner ring
                                                AND ST_Area(rg.geom) >= power(zres12, 2) -- 4 bigger than
                                            ) holes
                                ) new_geom
                        );
                    END IF;

                    IF ST_Area(geometry) < power(zres12, 2) THEN
                        CONTINUE;
                    END IF;

                    -- simplify
                    geometry := ST_SimplifyVW(geometry, zres14vw);

                    RETURN NEXT;
                END LOOP;
        END LOOP;
END;
$$ LANGUAGE plpgsql STABLE
                    STRICT
                    PARALLEL SAFE;


DROP MATERIALIZED VIEW IF EXISTS osm_building_block_gen1_dup CASCADE;

CREATE MATERIALIZED VIEW osm_building_block_gen1_dup AS
SELECT *
FROM osm_building_block_gen1();

CREATE INDEX ON osm_building_block_gen1_dup USING gist (geometry);

-- etldoc: osm_building_polygon -> osm_building_block_gen_z13
DROP MATERIALIZED VIEW IF EXISTS osm_building_block_gen_z13;
CREATE MATERIALIZED VIEW osm_building_block_gen_z13 AS
(
WITH 
    counts AS (
        SELECT count(osm_id) AS counts,
		        osm_id
	    FROM osm_building_block_gen1_dup
	GROUP BY osm_id
    ),

    duplicates AS (
        SELECT counts.osm_id
	    FROM counts
	    WHERE counts.counts > 1
    )

SELECT osm.osm_id,
		ST_Union(
            ST_MakeValid(osm.geometry)) AS geometry
	FROM osm_building_block_gen1_dup osm,
			duplicates
	WHERE osm.osm_id = duplicates.osm_id
	GROUP BY osm.osm_id
	
	UNION ALL

	SELECT osm.osm_id, 
			osm.geometry 
	FROM osm_building_block_gen1_dup osm, 
            counts 
	WHERE counts.counts = 1 
		AND osm.osm_id = counts.osm_id
);

CREATE INDEX ON osm_building_block_gen_z13 USING gist (geometry);
CREATE UNIQUE INDEX ON osm_building_block_gen_z13 USING btree (osm_id);

-- Handle updates

CREATE SCHEMA IF NOT EXISTS buildings;

CREATE TABLE IF NOT EXISTS buildings.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);

CREATE OR REPLACE FUNCTION buildings.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO buildings.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION buildings.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh buildings block';
    REFRESH MATERIALIZED VIEW osm_building_block_gen1_dup;
    REFRESH MATERIALIZED VIEW osm_building_block_gen_z13;
    -- noinspection SqlWithoutWhere
    DELETE FROM buildings.updates;

    RAISE LOG 'Update buildings block done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_flag
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_building_polygon
    FOR EACH STATEMENT
EXECUTE PROCEDURE buildings.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON buildings.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE buildings.refresh();

-- Layer building - ./building.sql

-- etldoc: layer_building[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="layer_building | <z13> z13 | <z14_> z14+ " ] ;

CREATE INDEX IF NOT EXISTS osm_building_relation_building_idx ON osm_building_relation (building) WHERE building = '' AND ST_GeometryType(geometry) = 'ST_Polygon';
CREATE INDEX IF NOT EXISTS osm_building_relation_member_idx ON osm_building_relation (member) WHERE role = 'outline';

CREATE OR REPLACE VIEW osm_all_buildings AS
(
SELECT
    -- etldoc: osm_building_relation -> layer_building:z14_
    -- Buildings built from relations
    member AS osm_id,
    geometry,
    COALESCE(CleanNumeric(height), CleanNumeric(buildingheight)) AS height,
    COALESCE(CleanNumeric(min_height), CleanNumeric(buildingmin_height)) AS min_height,
    COALESCE(CleanNumeric(levels), CleanNumeric(buildinglevels)) AS levels,
    COALESCE(CleanNumeric(min_level), CleanNumeric(buildingmin_level)) AS min_level,
    nullif(material, '') AS material,
    nullif(colour, '') AS colour,
    FALSE AS hide_3d
FROM osm_building_relation
WHERE building = ''
  AND ST_GeometryType(geometry) = 'ST_Polygon'
UNION ALL

SELECT
    -- etldoc: osm_building_polygon -> layer_building:z14_
    -- Standalone buildings
    obp.osm_id,
    obp.geometry,
    COALESCE(CleanNumeric(obp.height), CleanNumeric(obp.buildingheight)) AS height,
    COALESCE(CleanNumeric(obp.min_height), CleanNumeric(obp.buildingmin_height)) AS min_height,
    COALESCE(CleanNumeric(obp.levels), CleanNumeric(obp.buildinglevels)) AS levels,
    COALESCE(CleanNumeric(obp.min_level), CleanNumeric(obp.buildingmin_level)) AS min_level,
    nullif(obp.material, '') AS material,
    nullif(obp.colour, '') AS colour,
    obr.role IS NOT NULL AS hide_3d
FROM osm_building_polygon obp
         LEFT JOIN osm_building_relation obr ON
        obp.osm_id >= 0 AND
        obr.member = obp.osm_id AND
        obr.role = 'outline'
WHERE ST_GeometryType(obp.geometry) IN ('ST_Polygon', 'ST_MultiPolygon')
    );

CREATE OR REPLACE FUNCTION layer_building(bbox geometry, zoom_level int)
    RETURNS TABLE
            (
                geometry          geometry,
                osm_id            bigint,
                render_height     int,
                render_min_height int,
                colour            text,
                hide_3d           boolean
            )
AS
$$
SELECT geometry,
       osm_id,
       render_height,
       render_min_height,
       COALESCE(colour, CASE material
           -- Ordered by count from taginfo
                            WHEN 'cement_block' THEN '#6a7880'
                            WHEN 'brick' THEN '#bd8161'
                            WHEN 'plaster' THEN '#dadbdb'
                            WHEN 'wood' THEN '#d48741'
                            WHEN 'concrete' THEN '#d3c2b0'
                            WHEN 'metal' THEN '#b7b1a6'
                            WHEN 'stone' THEN '#b4a995'
                            WHEN 'mud' THEN '#9d8b75'
                            WHEN 'steel' THEN '#b7b1a6' -- same as metal
                            WHEN 'glass' THEN '#5a81a0'
                            WHEN 'traditional' THEN '#bd8161' -- same as brick
                            WHEN 'masonry' THEN '#bd8161' -- same as brick
                            WHEN 'Brick' THEN '#bd8161' -- same as brick
                            WHEN 'tin' THEN '#b7b1a6' -- same as metal
                            WHEN 'timber_framing' THEN '#b3b0a9'
                            WHEN 'sandstone' THEN '#b4a995' -- same as stone
                            WHEN 'clay' THEN '#9d8b75' -- same as mud
           END) AS colour,
       CASE WHEN hide_3d THEN TRUE END AS hide_3d
FROM (
         SELECT
             -- etldoc: osm_building_block_gen_z13 -> layer_building:z13
             osm_id,
             geometry,
             NULL::int AS render_height,
             NULL::int AS render_min_height,
             NULL::text AS material,
             NULL::text AS colour,
             FALSE AS hide_3d
         FROM osm_building_block_gen_z13
         WHERE zoom_level = 13
           AND geometry && bbox
         UNION ALL
         SELECT
                                  -- etldoc: osm_building_polygon -> layer_building:z14_
             DISTINCT ON (osm_id) osm_id,
                                  geometry,
                                  ceil(COALESCE(height, levels * 3.66, 5))::int AS render_height,
                                  floor(COALESCE(min_height, min_level * 3.66, 0))::int AS render_min_height,
                                  material,
                                  colour,
                                  hide_3d
         FROM osm_all_buildings
         WHERE (levels IS NULL OR levels < 1000)
           AND (min_level IS NULL OR min_level < 1000)
           AND (height IS NULL OR height < 3000)
           AND (min_height IS NULL OR min_height < 3000)
           AND zoom_level >= 14
           AND geometry && bbox
     ) AS zoom_levels
ORDER BY render_height ASC, ST_YMin(geometry) DESC;
$$ LANGUAGE SQL STABLE
                -- STRICT
                PARALLEL SAFE
                ;

-- not handled: where a building outline covers building parts

DO $$ BEGIN RAISE NOTICE 'Finished layer building'; END$$;
