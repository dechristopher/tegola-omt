DO $$ BEGIN RAISE NOTICE 'Processing layer boundary'; END$$;

-- Layer boundary - ./boundary_name.sql

DROP TABLE IF EXISTS osm_border_linestring_adm CASCADE;

-- etldoc: osm_border_linestring -> osm_border_linestring_adm
CREATE TABLE IF NOT EXISTS osm_border_linestring_adm AS ( 
  WITH 
    -- Prepare lines from osm to be merged
	multiline AS (
    	SELECT ST_Node(ST_Collect(geometry)) AS geometry, 
			   maritime,
			   disputed
    	FROM osm_border_linestring
    	WHERE admin_level = 2
		    AND osm_id NOT IN (SELECT DISTINCT osm_id FROM osm_border_disp_linestring)
		GROUP BY maritime,
				 disputed
		),

	mergedline AS (
		SELECT (ST_Dump(
      		     ST_LineMerge(geometry))).geom AS geometry,
			maritime,
			disputed
  		FROM multiline
		),
    -- Create polygons from all boundaries to preserve real shape of country
	polyg AS (
    	SELECT (ST_Dump(
        		 ST_Polygonize(geometry))).geom AS geometry  
    	FROM (
			SELECT (ST_Dump(
      				ST_LineMerge(geometry))).geom AS geometry
  			FROM (SELECT ST_Node(
                          ST_Collect(geometry)) AS geometry
    			FROM osm_border_linestring
    			WHERE admin_level = 2
                ) nodes
			) linemerge
  		), 

    centroids AS (
		SELECT polyg.geometry,
			   ne.adm0_a3
		FROM polyg,
			 ne_10m_admin_0_countries AS ne
		WHERE ST_Within(
			   ST_PointOnSurface(polyg.geometry), ne.geometry)
    	),

	country_osm_polyg AS  (
		SELECT country.adm0_a3,
			   border.geometry
  		FROM polyg border,
			 centroids country
  		WHERE ST_Within(country.geometry, border.geometry)
	),

	rights AS (
    	SELECT adm0_r,
			   geometry,
			   maritime,
			   disputed
		FROM (
			SELECT b.adm0_a3 AS adm0_r,
                   a.geometry,
                   a.maritime,
                   a.disputed
			FROM mergedline AS a
			LEFT JOIN country_osm_polyg AS b
            -- Create short line on the right of the boundary (mergedline) and find state where line lies.
			ON ST_Within(
				ST_OffsetCurve(
				(ST_LineSubString(a.geometry, 0.3,0.3004)), 70, 'quad_segs=4 join=mitre'), b.geometry)
            ) line_rights
		)

  SELECT adm0_l,
		 adm0_r,
		 geometry,
		 maritime,
		 2::integer AS admin_level,
		 disputed
  FROM (
    SELECT b.adm0_a3 AS adm0_l,
           r.adm0_r AS adm0_r,
           r.geometry,
           r.maritime,
           r.disputed
    FROM rights AS r
    LEFT JOIN country_osm_polyg AS b
      -- Create short line on the left of the boundary (mergedline) and find state where line lies.
      ON ST_Within(
        ST_OffsetCurve(
          (ST_LineSubString(r.geometry, 0.4,0.4004)), -70, 'quad_segs=4 join=mitre'), b.geometry)
    ) both_lines
);

CREATE INDEX IF NOT EXISTS osm_border_linestring_adm_geom_idx
  ON osm_border_linestring_adm
  USING GIST (geometry);

-- Layer boundary - ./boundary.sql

-- This statement can be deleted after the border importer image stops creating this object as a table
DO
$$
    BEGIN
        DROP TABLE IF EXISTS osm_border_linestring_gen_z13 CASCADE;
    EXCEPTION
        WHEN wrong_object_type THEN
    END;
$$ LANGUAGE plpgsql;

-- etldoc: osm_border_linestring -> osm_border_linestring_gen_z13
-- etldoc: osm_border_linestring_adm -> osm_border_linestring_gen_z13
DROP MATERIALIZED VIEW IF EXISTS osm_border_linestring_gen_z13 CASCADE;
CREATE MATERIALIZED VIEW osm_border_linestring_gen_z13 AS
(
SELECT ST_Simplify(geometry, ZRes(14)) AS geometry, NULL::text AS adm0_l, NULL::text AS adm0_r, admin_level, disputed, maritime
FROM osm_border_linestring
WHERE admin_level BETWEEN 3 AND 10
UNION ALL
SELECT ST_Simplify(geometry, ZRes(14)) AS geometry, adm0_l, adm0_r, admin_level, disputed, maritime
FROM osm_border_linestring_adm
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS osm_border_linestring_gen_z13_idx ON osm_border_linestring_gen_z13 USING gist (geometry);

-- This statement can be deleted after the border importer image stops creating this object as a table
DO
$$
    BEGIN
        DROP TABLE IF EXISTS osm_border_linestring_gen_z12 CASCADE;
    EXCEPTION
        WHEN wrong_object_type THEN
    END;
$$ LANGUAGE plpgsql;

-- etldoc: osm_border_linestring_gen_z13 -> osm_border_linestring_gen_z12
DROP MATERIALIZED VIEW IF EXISTS osm_border_linestring_gen_z12 CASCADE;
CREATE MATERIALIZED VIEW osm_border_linestring_gen_z12 AS
(
SELECT ST_Simplify(geometry, ZRes(13)) AS geometry, adm0_l, adm0_r, admin_level, disputed, maritime
FROM osm_border_linestring_gen_z13
WHERE admin_level BETWEEN 2 AND 10
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS osm_border_linestring_gen_z12_idx ON osm_border_linestring_gen_z12 USING gist (geometry);

-- This statement can be deleted after the border importer image stops creating this object as a table
DO
$$
    BEGIN
        DROP TABLE IF EXISTS osm_border_linestring_gen_z11 CASCADE;
    EXCEPTION
        WHEN wrong_object_type THEN
    END;
$$ LANGUAGE plpgsql;

-- etldoc: osm_border_linestring_gen_z12 -> osm_border_linestring_gen_z11
DROP MATERIALIZED VIEW IF EXISTS osm_border_linestring_gen_z11 CASCADE;
CREATE MATERIALIZED VIEW osm_border_linestring_gen_z11 AS
(
SELECT ST_Simplify(geometry, ZRes(12)) AS geometry, adm0_l, adm0_r, admin_level, disputed, maritime
FROM osm_border_linestring_gen_z12
WHERE admin_level BETWEEN 2 AND 8
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS osm_border_linestring_gen_z11_idx ON osm_border_linestring_gen_z11 USING gist (geometry);

-- This statement can be deleted after the border importer image stops creating this object as a table
DO
$$
    BEGIN
        DROP TABLE IF EXISTS osm_border_linestring_gen_z10 CASCADE;
    EXCEPTION
        WHEN wrong_object_type THEN
    END;
$$ LANGUAGE plpgsql;

-- etldoc: osm_border_linestring_gen_z11 -> osm_border_linestring_gen_z10
DROP MATERIALIZED VIEW IF EXISTS osm_border_linestring_gen_z10 CASCADE;
CREATE MATERIALIZED VIEW osm_border_linestring_gen_z10 AS
(
SELECT ST_Simplify(geometry, ZRes(11)) AS geometry, adm0_l, adm0_r, admin_level, disputed, maritime
FROM osm_border_linestring_gen_z11
WHERE admin_level BETWEEN 2 AND 6
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS osm_border_linestring_gen_z10_idx ON osm_border_linestring_gen_z10 USING gist (geometry);

-- This statement can be deleted after the border importer image stops creating this object as a table
DO
$$
    BEGIN
        DROP TABLE IF EXISTS osm_border_linestring_gen_z9 CASCADE;
    EXCEPTION
        WHEN wrong_object_type THEN
    END;
$$ LANGUAGE plpgsql;

-- etldoc: osm_border_linestring_gen_z10 -> osm_border_linestring_gen_z9
DROP MATERIALIZED VIEW IF EXISTS osm_border_linestring_gen_z9 CASCADE;
CREATE MATERIALIZED VIEW osm_border_linestring_gen_z9 AS
(
SELECT ST_Simplify(geometry, ZRes(10)) AS geometry, adm0_l, adm0_r, admin_level, disputed, maritime
FROM osm_border_linestring_gen_z10
WHERE admin_level BETWEEN 2 AND 6
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS osm_border_linestring_gen_z9_idx ON osm_border_linestring_gen_z9 USING gist (geometry);

-- This statement can be deleted after the border importer image stops creating this object as a table
DO
$$
    BEGIN
        DROP TABLE IF EXISTS osm_border_linestring_gen_z8 CASCADE;
    EXCEPTION
        WHEN wrong_object_type THEN
    END;
$$ LANGUAGE plpgsql;

-- etldoc: osm_border_linestring_gen_z9 -> osm_border_linestring_gen_z8
DROP MATERIALIZED VIEW IF EXISTS osm_border_linestring_gen_z8 CASCADE;
CREATE MATERIALIZED VIEW osm_border_linestring_gen_z8 AS
(
SELECT ST_Simplify(geometry, ZRes(9)) AS geometry, adm0_l, adm0_r, admin_level, disputed, maritime
FROM osm_border_linestring_gen_z9
WHERE admin_level BETWEEN 2 AND 4
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS osm_border_linestring_gen_z8_idx ON osm_border_linestring_gen_z8 USING gist (geometry);

-- This statement can be deleted after the border importer image stops creating this object as a table
DO
$$
    BEGIN
        DROP TABLE IF EXISTS osm_border_linestring_gen_z7 CASCADE;
    EXCEPTION
        WHEN wrong_object_type THEN
    END;
$$ LANGUAGE plpgsql;

-- etldoc: osm_border_linestring_gen_z8 -> osm_border_linestring_gen_z7
DROP MATERIALIZED VIEW IF EXISTS osm_border_linestring_gen_z7 CASCADE;
CREATE MATERIALIZED VIEW osm_border_linestring_gen_z7 AS
(
SELECT ST_Simplify(geometry, ZRes(8)) AS geometry, adm0_l, adm0_r, admin_level, disputed, maritime
FROM osm_border_linestring_gen_z8
WHERE admin_level BETWEEN 2 AND 4
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS osm_border_linestring_gen_z7_idx ON osm_border_linestring_gen_z7 USING gist (geometry);

-- This statement can be deleted after the border importer image stops creating this object as a table
DO
$$
    BEGIN
        DROP TABLE IF EXISTS osm_border_linestring_gen_z6 CASCADE;
    EXCEPTION
        WHEN wrong_object_type THEN
    END;
$$ LANGUAGE plpgsql;

-- etldoc: osm_border_linestring_gen_z7 -> osm_border_linestring_gen_z6
DROP MATERIALIZED VIEW IF EXISTS osm_border_linestring_gen_z6 CASCADE;
CREATE MATERIALIZED VIEW osm_border_linestring_gen_z6 AS
(
SELECT ST_Simplify(geometry, ZRes(7)) AS geometry, adm0_l, adm0_r, admin_level, disputed, maritime
FROM osm_border_linestring_gen_z7
WHERE admin_level BETWEEN 2 AND 4
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS osm_border_linestring_gen_z6_idx ON osm_border_linestring_gen_z6 USING gist (geometry);

-- This statement can be deleted after the border importer image stops creating this object as a table
DO
$$
    BEGIN
        DROP TABLE IF EXISTS osm_border_linestring_gen_z5 CASCADE;
    EXCEPTION
        WHEN wrong_object_type THEN
    END;
$$ LANGUAGE plpgsql;

-- etldoc: osm_border_linestring_gen_z6 -> osm_border_linestring_gen_z5
DROP MATERIALIZED VIEW IF EXISTS osm_border_linestring_gen_z5 CASCADE;
CREATE MATERIALIZED VIEW osm_border_linestring_gen_z5 AS
(
SELECT ST_Simplify(geometry, ZRes(6)) AS geometry, adm0_l, adm0_r, admin_level, disputed, maritime
FROM osm_border_linestring_gen_z6
WHERE admin_level BETWEEN 2 AND 4
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS osm_border_linestring_gen_z5_idx ON osm_border_linestring_gen_z5 USING gist (geometry);

-- This statement can be deleted after the border importer image stops creating this object as a table
DO
$$
    BEGIN
        DROP TABLE IF EXISTS osm_border_linestring_gen_z4 CASCADE;
    EXCEPTION
        WHEN wrong_object_type THEN
    END;
$$ LANGUAGE plpgsql;

-- etldoc: osm_border_linestring_gen_z5 -> osm_border_linestring_gen_z4
DROP MATERIALIZED VIEW IF EXISTS osm_border_linestring_gen_z4 CASCADE;
CREATE MATERIALIZED VIEW osm_border_linestring_gen_z4 AS
(
SELECT ST_Simplify(geometry, ZRes(5)) AS geometry, adm0_l, adm0_r, admin_level, disputed, maritime
FROM osm_border_linestring_gen_z5
WHERE admin_level = 2
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS osm_border_linestring_gen_z4_idx ON osm_border_linestring_gen_z4 USING gist (geometry);

-- ne_10m_admin_0_boundary_lines_land
-- etldoc: ne_10m_admin_0_boundary_lines_land -> ne_10m_admin_0_boundary_lines_land_gen_z4
DROP MATERIALIZED VIEW IF EXISTS ne_10m_admin_0_boundary_lines_land_gen_z4 CASCADE;
CREATE MATERIALIZED VIEW ne_10m_admin_0_boundary_lines_land_gen_z4 AS
(
SELECT ST_Simplify(geometry, ZRes(6)) as geometry,
       2 AS admin_level,
       (CASE WHEN featurecla LIKE 'Disputed%' THEN TRUE ELSE FALSE END) AS disputed,
       (CASE WHEN featurecla LIKE 'Disputed%' THEN 'ne10m_' || ogc_fid ELSE NULL::text END) AS disputed_name,
       NULL::text AS claimed_by,
       FALSE AS maritime
FROM ne_10m_admin_0_boundary_lines_land
WHERE featurecla <> 'Lease limit'
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS ne_10m_admin_0_boundary_lines_land_gen_z4_idx ON ne_10m_admin_0_boundary_lines_land_gen_z4 USING gist (geometry);

-- ne_10m_admin_1_states_provinces_lines
-- etldoc: ne_10m_admin_1_states_provinces_lines -> ne_10m_admin_1_states_provinces_lines_gen_z4
DROP MATERIALIZED VIEW IF EXISTS ne_10m_admin_1_states_provinces_lines_gen_z4 CASCADE;
CREATE MATERIALIZED VIEW ne_10m_admin_1_states_provinces_lines_gen_z4 AS
(
SELECT ST_Simplify(geometry, ZRes(6)) as geometry,
       4 AS admin_level,
       FALSE AS disputed,
       NULL::text AS disputed_name,
       NULL::text AS claimed_by,
       FALSE AS maritime
FROM ne_10m_admin_1_states_provinces_lines
WHERE min_zoom <= 7
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS ne_10m_admin_1_states_provinces_lines_gen_z4_idx ON ne_10m_admin_1_states_provinces_lines_gen_z4 USING gist (geometry);


-- etldoc: ne_10m_admin_1_states_provinces_lines_gen_z4 -> ne_10m_admin_1_states_provinces_lines_gen_z3
DROP MATERIALIZED VIEW IF EXISTS ne_10m_admin_1_states_provinces_lines_gen_z3 CASCADE;
CREATE MATERIALIZED VIEW ne_10m_admin_1_states_provinces_lines_gen_z3 AS
(
SELECT ST_Simplify(geometry, ZRes(5)) as geometry,
       admin_level,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_10m_admin_1_states_provinces_lines_gen_z4
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS ne_10m_admin_1_states_provinces_lines_gen_z3_idx ON ne_10m_admin_1_states_provinces_lines_gen_z3 USING gist (geometry);

-- etldoc: ne_10m_admin_1_states_provinces_lines_gen_z3 -> ne_10m_admin_1_states_provinces_lines_gen_z2
DROP MATERIALIZED VIEW IF EXISTS ne_10m_admin_1_states_provinces_lines_gen_z2 CASCADE;
CREATE MATERIALIZED VIEW ne_10m_admin_1_states_provinces_lines_gen_z2 AS
(
SELECT ST_Simplify(geometry, ZRes(4)) as geometry,
       admin_level,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_10m_admin_1_states_provinces_lines_gen_z3
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS ne_10m_admin_1_states_provinces_lines_gen_z2_idx ON ne_10m_admin_1_states_provinces_lines_gen_z2 USING gist (geometry);

-- etldoc: ne_10m_admin_1_states_provinces_lines_gen_z2 -> ne_10m_admin_1_states_provinces_lines_gen_z1
DROP MATERIALIZED VIEW IF EXISTS ne_10m_admin_1_states_provinces_lines_gen_z1 CASCADE;
CREATE MATERIALIZED VIEW ne_10m_admin_1_states_provinces_lines_gen_z1 AS
(
SELECT ST_Simplify(geometry, ZRes(3)) as geometry,
       admin_level,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_10m_admin_1_states_provinces_lines_gen_z2
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS ne_10m_admin_1_states_provinces_lines_gen_z1_idx ON ne_10m_admin_1_states_provinces_lines_gen_z1 USING gist (geometry);

-- ne_50m_admin_0_boundary_lines_land
-- etldoc: ne_50m_admin_0_boundary_lines_land -> ne_50m_admin_0_boundary_lines_land_gen_z3
DROP MATERIALIZED VIEW IF EXISTS ne_50m_admin_0_boundary_lines_land_gen_z3 CASCADE;
CREATE MATERIALIZED VIEW ne_50m_admin_0_boundary_lines_land_gen_z3 AS
(
SELECT ST_Simplify(geometry, ZRes(5)) as geometry,
       2 AS admin_level,
       (CASE WHEN featurecla LIKE 'Disputed%' THEN TRUE ELSE FALSE END) AS disputed,
       (CASE WHEN featurecla LIKE 'Disputed%' THEN 'ne50m_' || ogc_fid ELSE NULL::text END) AS disputed_name,
       NULL::text AS claimed_by,
       FALSE AS maritime
FROM ne_50m_admin_0_boundary_lines_land
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS ne_50m_admin_0_boundary_lines_land_gen_z3_idx ON ne_50m_admin_0_boundary_lines_land_gen_z3 USING gist (geometry);

-- etldoc: ne_50m_admin_0_boundary_lines_land_gen_z3 -> ne_50m_admin_0_boundary_lines_land_gen_z2
DROP MATERIALIZED VIEW IF EXISTS ne_50m_admin_0_boundary_lines_land_gen_z2 CASCADE;
CREATE MATERIALIZED VIEW ne_50m_admin_0_boundary_lines_land_gen_z2 AS
(
SELECT ST_Simplify(geometry, ZRes(4)) as geometry,
       admin_level,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_50m_admin_0_boundary_lines_land_gen_z3
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS ne_50m_admin_0_boundary_lines_land_gen_z2_idx ON ne_50m_admin_0_boundary_lines_land_gen_z2 USING gist (geometry);

-- etldoc: ne_50m_admin_0_boundary_lines_land_gen_z2 -> ne_50m_admin_0_boundary_lines_land_gen_z1
DROP MATERIALIZED VIEW IF EXISTS ne_50m_admin_0_boundary_lines_land_gen_z1 CASCADE;
CREATE MATERIALIZED VIEW ne_50m_admin_0_boundary_lines_land_gen_z1 AS
(
SELECT ST_Simplify(geometry, ZRes(3)) as geometry,
       admin_level,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_50m_admin_0_boundary_lines_land_gen_z2
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS ne_50m_admin_0_boundary_lines_land_gen_z1_idx ON ne_50m_admin_0_boundary_lines_land_gen_z1 USING gist (geometry);

-- ne_110m_admin_0_boundary_lines_land
-- etldoc: ne_110m_admin_0_boundary_lines_land -> ne_110m_admin_0_boundary_lines_land_gen_z0
DROP MATERIALIZED VIEW IF EXISTS ne_110m_admin_0_boundary_lines_land_gen_z0 CASCADE;
CREATE MATERIALIZED VIEW ne_110m_admin_0_boundary_lines_land_gen_z0 AS
(
SELECT ST_Simplify(geometry, ZRes(2)) as geometry,
       2 AS admin_level,
       (CASE WHEN featurecla LIKE 'Disputed%' THEN TRUE ELSE FALSE END) AS disputed,
       (CASE WHEN featurecla LIKE 'Disputed%' THEN 'ne110m_' || ogc_fid ELSE NULL::text END) AS disputed_name,
       NULL::text AS claimed_by,
       FALSE AS maritime
FROM ne_110m_admin_0_boundary_lines_land
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */ ;
CREATE INDEX IF NOT EXISTS ne_110m_admin_0_boundary_lines_land_gen_z0_idx ON ne_110m_admin_0_boundary_lines_land_gen_z0 USING gist (geometry);


CREATE OR REPLACE FUNCTION edit_name(name varchar) RETURNS text AS
$$
SELECT CASE
           WHEN POSITION(' at ' IN name) > 0
               THEN replace(SUBSTRING(name, POSITION(' at ' IN name) + 4), ' ', '')
           ELSE replace(replace(name, ' ', ''), 'Extentof', '')
           END;
$$ LANGUAGE SQL IMMUTABLE
                -- STRICT
                PARALLEL SAFE
                ;


-- etldoc: ne_110m_admin_0_boundary_lines_land_gen_z0  -> boundary_z0
CREATE OR REPLACE VIEW boundary_z0 AS
(
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_110m_admin_0_boundary_lines_land_gen_z0
    );

-- etldoc: ne_50m_admin_0_boundary_lines_land_gen_z1  -> boundary_z1
-- etldoc: ne_10m_admin_1_states_provinces_lines_gen_z1 -> boundary_z1
-- etldoc: osm_border_disp_linestring_gen_z1 -> boundary_z1
CREATE OR REPLACE VIEW boundary_z1 AS
(
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_50m_admin_0_boundary_lines_land_gen_z1
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_10m_admin_1_states_provinces_lines_gen_z1
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z1
    );


-- etldoc: ne_50m_admin_0_boundary_lines_land_gen_z2 -> boundary_z2
-- etldoc: ne_10m_admin_1_states_provinces_lines_gen_z2 -> boundary_z2
-- etldoc: osm_border_disp_linestring_gen_z2 -> boundary_z2
CREATE OR REPLACE VIEW boundary_z2 AS
(
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_50m_admin_0_boundary_lines_land_gen_z2
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_10m_admin_1_states_provinces_lines_gen_z2
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z2
    );

-- etldoc: ne_50m_admin_0_boundary_lines_land_gen_z3 -> boundary_z3
-- etldoc: ne_10m_admin_1_states_provinces_lines_gen_z3 -> boundary_z3
-- etldoc: osm_border_disp_linestring_gen_z3 -> boundary_z3
CREATE OR REPLACE VIEW boundary_z3 AS
(
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_50m_admin_0_boundary_lines_land_gen_z3
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_10m_admin_1_states_provinces_lines_gen_z3
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z3
    );

-- etldoc: ne_10m_admin_0_boundary_lines_land_gen_z4 -> boundary_z4
-- etldoc: ne_10m_admin_1_states_provinces_lines_gen_z4 -> boundary_z4
-- etldoc: osm_border_linestring_gen_z4 -> boundary_z4
-- etldoc: osm_border_disp_linestring_gen_z4 -> boundary_z4
CREATE OR REPLACE VIEW boundary_z4 AS
(
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_10m_admin_0_boundary_lines_land_gen_z4
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       disputed,
       disputed_name,
       claimed_by,
       maritime
FROM ne_10m_admin_1_states_provinces_lines_gen_z4
UNION ALL
SELECT geometry,
       admin_level,
       adm0_l,
       adm0_r,
       disputed,
       NULL::text AS disputed_name,
       NULL::text AS claimed_by,
       maritime
FROM osm_border_linestring_gen_z4
WHERE maritime = TRUE
  AND admin_level <= 2
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z4
    );

-- etldoc: osm_border_linestring_gen_z5 -> boundary_z5
-- etldoc: osm_border_disp_linestring_gen_z5 -> boundary_z5
CREATE OR REPLACE VIEW boundary_z5 AS
(
SELECT geometry,
       admin_level,
       adm0_l,
       adm0_r,
       disputed,
       NULL::text AS disputed_name,
       NULL::text AS claimed_by,
       maritime
FROM osm_border_linestring_gen_z5
WHERE admin_level <= 4
-- already not included in osm_border_linestring_adm
--  AND osm_id NOT IN (SELECT DISTINCT osm_id FROM osm_border_disp_linestring_gen_z5)
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z5
    );

-- etldoc: osm_border_linestring_gen_z6 -> boundary_z6
-- etldoc: osm_border_disp_linestring_gen_z6 -> boundary_z6
CREATE OR REPLACE VIEW boundary_z6 AS
(
SELECT geometry,
       admin_level,
       adm0_l,
       adm0_r,
       disputed,
       NULL::text AS disputed_name,
       NULL::text AS claimed_by,
       maritime
FROM osm_border_linestring_gen_z6
WHERE admin_level <= 4
--  AND osm_id NOT IN (SELECT DISTINCT osm_id FROM osm_border_disp_linestring_gen_z6)
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z6
    );

-- etldoc: osm_border_linestring_gen_z7 -> boundary_z7
-- etldoc: osm_border_disp_linestring_gen_z7 -> boundary_z7
CREATE OR REPLACE VIEW boundary_z7 AS
(
SELECT geometry,
       admin_level,
       adm0_l,
       adm0_r,
       disputed,
       NULL::text AS disputed_name,
       NULL::text AS claimed_by,
       maritime
FROM osm_border_linestring_gen_z7
WHERE admin_level <= 6
--  AND osm_id NOT IN (SELECT DISTINCT osm_id FROM osm_border_disp_linestring_gen_z7)
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z7
    );

-- etldoc: osm_border_linestring_gen_z8 -> boundary_z8
-- etldoc: osm_border_disp_linestring_gen_z8 -> boundary_z8
CREATE OR REPLACE VIEW boundary_z8 AS
(
SELECT geometry,
       admin_level,
       adm0_l,
       adm0_r,
       disputed,
       NULL::text AS disputed_name,
       NULL::text AS claimed_by,
       maritime
FROM osm_border_linestring_gen_z8
WHERE admin_level <= 6
--  AND osm_id NOT IN (SELECT DISTINCT osm_id FROM osm_border_disp_linestring_gen_z8)
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z8
    );

-- etldoc: osm_border_linestring_gen_z9 -> boundary_z9
-- etldoc: osm_border_disp_linestring_gen_z9 -> boundary_z9
CREATE OR REPLACE VIEW boundary_z9 AS
(
SELECT geometry,
       admin_level,
       adm0_l,
       adm0_r,
       disputed,
       NULL::text AS disputed_name,
       NULL::text AS claimed_by,
       maritime
FROM osm_border_linestring_gen_z9
WHERE admin_level <= 6
--  AND osm_id NOT IN (SELECT DISTINCT osm_id FROM osm_border_disp_linestring_gen_z9)
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z9
    );

-- etldoc: osm_border_linestring_gen_z10 -> boundary_z10
-- etldoc: osm_border_disp_linestring_gen_z10 -> boundary_z10
CREATE OR REPLACE VIEW boundary_z10 AS
(
SELECT geometry,
       admin_level,
       adm0_l,
       adm0_r,
       disputed,
       NULL::text AS disputed_name,
       NULL::text AS claimed_by,
       maritime
FROM osm_border_linestring_gen_z10
WHERE admin_level <= 6
--  AND osm_id NOT IN (SELECT DISTINCT osm_id FROM osm_border_disp_linestring_gen_z10)
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z10
    );

-- etldoc: osm_border_linestring_gen_z11 -> boundary_z11
-- etldoc: osm_border_disp_linestring_gen_z11 -> boundary_z11
CREATE OR REPLACE VIEW boundary_z11 AS
(
SELECT geometry,
       admin_level,
       adm0_l,
       adm0_r,
       disputed,
       NULL::text AS disputed_name,
       NULL::text AS claimed_by,
       maritime
FROM osm_border_linestring_gen_z11
WHERE admin_level <= 8
--  AND osm_id NOT IN (SELECT DISTINCT osm_id FROM osm_border_disp_linestring_gen_z11)
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z11
    );

-- etldoc: osm_border_linestring_gen_z12 -> boundary_z12
-- etldoc: osm_border_disp_linestring_gen_z12 -> boundary_z12
CREATE OR REPLACE VIEW boundary_z12 AS
(
SELECT geometry,
       admin_level,
       adm0_l,
       adm0_r,
       disputed,
       NULL::text AS disputed_name,
       NULL::text AS claimed_by,
       maritime
FROM osm_border_linestring_gen_z12
--WHERE osm_id NOT IN (SELECT DISTINCT osm_id FROM osm_border_disp_linestring_gen_z12)
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z12
    );

-- etldoc: osm_border_linestring_gen_z13 -> boundary_z13
-- etldoc: osm_border_disp_linestring_gen_z13 -> boundary_z13
CREATE OR REPLACE VIEW boundary_z13 AS
(
SELECT geometry,
       admin_level,
       adm0_l,
       adm0_r,
       disputed,
       NULL::text AS disputed_name,
       NULL::text AS claimed_by,
       maritime
FROM osm_border_linestring_gen_z13
--WHERE osm_id NOT IN (SELECT DISTINCT osm_id FROM osm_border_disp_linestring_gen_z13)
UNION ALL
SELECT geometry,
       admin_level,
       NULL::text AS adm0_l,
       NULL::text AS adm0_r,
       TRUE AS disputed,
       edit_name(name) AS disputed_name,
       claimed_by,
       maritime
FROM osm_border_disp_linestring_gen_z13
    );

-- etldoc: layer_boundary[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="<sql> layer_boundary |<z0> z0 |<z1> z1 |<z2> z2 | <z3> z3 | <z4> z4 | <z5> z5 | <z6> z6 | <z7> z7 | <z8> z8 | <z9> z9 |<z10> z10 |<z11> z11 |<z12> z12|<z13> z13+"]
CREATE OR REPLACE FUNCTION layer_boundary(bbox geometry, zoom_level int)
    RETURNS TABLE
            (
                geometry      geometry,
                admin_level   int,
                adm0_l        text,
                adm0_r        text,
                disputed      int,
                disputed_name text,
                claimed_by    text,
                maritime      int
            )
AS
$$
SELECT geometry, admin_level, adm0_l, adm0_r, disputed::int, disputed_name, claimed_by, maritime::int
FROM (
         -- etldoc: boundary_z0 ->  layer_boundary:z0
         SELECT *
         FROM boundary_z0
         WHERE geometry && bbox
           AND zoom_level = 0
         UNION ALL
         -- etldoc: boundary_z1 ->  layer_boundary:z1
         SELECT *
         FROM boundary_z1
         WHERE geometry && bbox
           AND zoom_level = 1
         UNION ALL
         -- etldoc: boundary_z2 ->  layer_boundary:z2
         SELECT *
         FROM boundary_z2
         WHERE geometry && bbox
           AND zoom_level = 2
         UNION ALL
         -- etldoc: boundary_z3 ->  layer_boundary:z3
         SELECT *
         FROM boundary_z3
         WHERE geometry && bbox
           AND zoom_level = 3
         UNION ALL
         -- etldoc: boundary_z4 ->  layer_boundary:z4
         SELECT *
         FROM boundary_z4
         WHERE geometry && bbox
           AND zoom_level = 4
         UNION ALL
         -- etldoc: boundary_z5 ->  layer_boundary:z5
         SELECT *
         FROM boundary_z5
         WHERE geometry && bbox
           AND zoom_level = 5
         UNION ALL
         -- etldoc: boundary_z6 ->  layer_boundary:z6
         SELECT *
         FROM boundary_z6
         WHERE geometry && bbox
           AND zoom_level = 6
         UNION ALL
         -- etldoc: boundary_z7 ->  layer_boundary:z7
         SELECT *
         FROM boundary_z7
         WHERE geometry && bbox
           AND zoom_level = 7
         UNION ALL
         -- etldoc: boundary_z8 ->  layer_boundary:z8
         SELECT *
         FROM boundary_z8
         WHERE geometry && bbox
           AND zoom_level = 8
         UNION ALL
         -- etldoc: boundary_z9 ->  layer_boundary:z9
         SELECT *
         FROM boundary_z9
         WHERE geometry && bbox
           AND zoom_level = 9
         UNION ALL
         -- etldoc: boundary_z10 ->  layer_boundary:z10
         SELECT *
         FROM boundary_z10
         WHERE geometry && bbox
           AND zoom_level = 10
         UNION ALL
         -- etldoc: boundary_z11 ->  layer_boundary:z11
         SELECT *
         FROM boundary_z11
         WHERE geometry && bbox
           AND zoom_level = 11
         UNION ALL
         -- etldoc: boundary_z12 ->  layer_boundary:z12
         SELECT *
         FROM boundary_z12
         WHERE geometry && bbox
           AND zoom_level = 12
         UNION ALL
         -- etldoc: boundary_z13 -> layer_boundary:z13
         SELECT *
         FROM boundary_z13
         WHERE geometry && bbox
           AND zoom_level >= 13
     ) AS zoom_levels;
$$ LANGUAGE SQL STABLE
                -- STRICT
                PARALLEL SAFE;

DO $$ BEGIN RAISE NOTICE 'Finished layer boundary'; END$$;
