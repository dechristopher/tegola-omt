DO $$ BEGIN RAISE NOTICE 'Processing layer poi'; END$$;

-- Layer poi - ./public_transport_stop_type.sql

DO
$$
    BEGIN
        IF NOT EXISTS(SELECT 1
                      FROM pg_type
                      WHERE typname = 'public_transport_stop_type') THEN
            CREATE TYPE public_transport_stop_type AS enum (
                'subway', 'tram_stop', 'bus_station', 'bus_stop'
                );
        END IF;
    END
$$;

-- Layer poi - ./class.sql

CREATE OR REPLACE FUNCTION poi_class_rank(class text)
    RETURNS int AS
$$
SELECT CASE class
           WHEN 'hospital' THEN 20
           WHEN 'railway' THEN 40
           WHEN 'bus' THEN 50
           WHEN 'attraction' THEN 70
           WHEN 'harbor' THEN 75
           WHEN 'college' THEN 80
           WHEN 'school' THEN 85
           WHEN 'stadium' THEN 90
           WHEN 'zoo' THEN 95
           WHEN 'town_hall' THEN 100
           WHEN 'campsite' THEN 110
           WHEN 'cemetery' THEN 115
           WHEN 'park' THEN 120
           WHEN 'library' THEN 130
           WHEN 'police' THEN 135
           WHEN 'post' THEN 140
           WHEN 'golf' THEN 150
           WHEN 'shop' THEN 400
           WHEN 'grocery' THEN 500
           WHEN 'fast_food' THEN 600
           WHEN 'clothing_store' THEN 700
           WHEN 'bar' THEN 800
           ELSE 1000
           END;
$$ LANGUAGE SQL IMMUTABLE
                PARALLEL SAFE;

CREATE OR REPLACE FUNCTION poi_class(subclass text, mapping_key text)
    RETURNS text AS
$$
SELECT CASE
           WHEN "subclass" IN ('accessories', 'antiques', 'beauty', 'bed', 'boutique', 'camera', 'carpet', 'charity', 'chemist', 'coffee', 'computer', 'convenience', 'copyshop', 'cosmetics', 'garden_centre', 'doityourself', 'erotic', 'electronics', 'fabric', 'florist', 'frozen_food', 'furniture', 'video_games', 'video', 'general', 'gift', 'hardware', 'hearing_aids', 'hifi', 'ice_cream', 'interior_decoration', 'jewelry', 'kiosk', 'lamps', 'mall', 'massage', 'motorcycle', 'mobile_phone', 'newsagent', 'optician', 'outdoor', 'perfumery', 'perfume', 'pet', 'photo', 'second_hand', 'shoes', 'sports', 'stationery', 'tailor', 'tattoo', 'ticket', 'tobacco', 'toys', 'travel_agency', 'watches', 'weapons', 'wholesale') THEN 'shop'
           WHEN "subclass" IN ('townhall', 'public_building', 'courthouse', 'community_centre') THEN 'town_hall'
           WHEN "subclass" IN ('golf', 'golf_course', 'miniature_golf') THEN 'golf'
           WHEN "subclass" IN ('fast_food', 'food_court') THEN 'fast_food'
           WHEN "subclass" IN ('park', 'bbq') THEN 'park'
           WHEN "subclass" IN ('bus_stop', 'bus_station') THEN 'bus'
           WHEN ("subclass" = 'station' AND "mapping_key" = 'railway')
               OR "subclass" IN ('halt', 'tram_stop', 'subway')
               THEN 'railway'
           WHEN "subclass" = 'station'
               AND "mapping_key" = 'aerialway'
               THEN 'aerialway'
           WHEN "subclass" IN ('subway_entrance', 'train_station_entrance') THEN 'entrance'
           WHEN "subclass" IN ('camp_site', 'caravan_site') THEN 'campsite'
           WHEN "subclass" IN ('laundry', 'dry_cleaning') THEN 'laundry'
           WHEN "subclass" IN ('supermarket', 'deli', 'delicatessen', 'department_store', 'greengrocer', 'marketplace') THEN 'grocery'
           WHEN "subclass" IN ('books', 'library') THEN 'library'
           WHEN "subclass" IN ('university', 'college') THEN 'college'
           WHEN "subclass" IN ('hotel', 'motel', 'bed_and_breakfast', 'guest_house', 'hostel', 'chalet', 'alpine_hut', 'dormitory') THEN 'lodging'
           WHEN "subclass" IN ('chocolate', 'confectionery') THEN 'ice_cream'
           WHEN "subclass" IN ('post_box', 'post_office') THEN 'post'
           WHEN "subclass" = 'cafe' THEN 'cafe'
           WHEN "subclass" IN ('school', 'kindergarten') THEN 'school'
           WHEN "subclass" IN ('alcohol', 'beverages', 'wine') THEN 'alcohol_shop'
           WHEN "subclass" IN ('bar', 'nightclub') THEN 'bar'
           WHEN "subclass" IN ('marina', 'dock') THEN 'harbor'
           WHEN "subclass" IN ('car', 'car_repair', 'car_parts', 'taxi') THEN 'car'
           WHEN "subclass" IN ('hospital', 'nursing_home', 'clinic') THEN 'hospital'
           WHEN "subclass" IN ('grave_yard', 'cemetery') THEN 'cemetery'
           WHEN "subclass" IN ('attraction', 'viewpoint') THEN 'attraction'
           WHEN "subclass" IN ('biergarten', 'pub') THEN 'beer'
           WHEN "subclass" IN ('music', 'musical_instrument') THEN 'music'
           WHEN "subclass" IN ('american_football', 'stadium', 'soccer') THEN 'stadium'
           WHEN "subclass" IN ('art', 'artwork', 'gallery', 'arts_centre') THEN 'art_gallery'
           WHEN "subclass" IN ('bag', 'clothes') THEN 'clothing_store'
           WHEN "subclass" IN ('swimming_area', 'swimming') THEN 'swimming'
           WHEN "subclass" IN ('castle', 'ruins') THEN 'castle'
           ELSE subclass
           END;
$$ LANGUAGE SQL IMMUTABLE
                PARALLEL SAFE;

-- Layer poi - ./poi_stop_agg.sql

DROP MATERIALIZED VIEW IF EXISTS osm_poi_stop_centroid CASCADE;
CREATE MATERIALIZED VIEW osm_poi_stop_centroid AS
(
SELECT uic_ref,
       count(*) AS count,
       CASE WHEN count(*) > 2 THEN ST_Centroid(ST_UNION(geometry)) END AS centroid
FROM osm_poi_point
WHERE nullif(uic_ref, '') IS NOT NULL
  AND subclass IN ('bus_stop', 'bus_station', 'tram_stop', 'subway')
GROUP BY uic_ref
HAVING count(*) > 1
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;

DROP MATERIALIZED VIEW IF EXISTS osm_poi_stop_rank CASCADE;
CREATE MATERIALIZED VIEW osm_poi_stop_rank AS
(
SELECT p.osm_id,
-- 		p.uic_ref,
-- 		p.subclass,
       ROW_NUMBER()
       OVER (
           PARTITION BY p.uic_ref
           ORDER BY
               p.subclass :: public_transport_stop_type NULLS LAST,
               ST_Distance(c.centroid, p.geometry)
           ) AS rk
FROM osm_poi_point p
         INNER JOIN osm_poi_stop_centroid c ON (p.uic_ref = c.uic_ref)
WHERE subclass IN ('bus_stop', 'bus_station', 'tram_stop', 'subway')
ORDER BY p.uic_ref, rk
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;

-- Layer poi - ./update_poi_polygon.sql

DROP TRIGGER IF EXISTS trigger_flag ON osm_poi_polygon;
DROP TRIGGER IF EXISTS trigger_store ON osm_poi_polygon;
DROP TRIGGER IF EXISTS trigger_refresh ON poi_polygon.updates;

CREATE SCHEMA IF NOT EXISTS poi_polygon;

CREATE TABLE IF NOT EXISTS poi_polygon.osm_ids
(
    osm_id bigint
);

-- etldoc:  osm_poi_polygon ->  osm_poi_polygon

CREATE OR REPLACE FUNCTION update_poi_polygon(full_update boolean) RETURNS void AS
$$
    UPDATE osm_poi_polygon
    SET geometry =
            CASE
                WHEN ST_NPoints(ST_ConvexHull(geometry)) = ST_NPoints(geometry)
                    THEN ST_Centroid(geometry)
                ELSE ST_PointOnSurface(geometry)
                END
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM poi_polygon.osm_ids))
      AND ST_GeometryType(geometry) <> 'ST_Point'
      AND ST_IsValid(geometry);

    UPDATE osm_poi_polygon
    SET subclass = 'subway'
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM poi_polygon.osm_ids))
      AND station = 'subway'
      AND subclass = 'station';

    UPDATE osm_poi_polygon
    SET subclass = 'halt'
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM poi_polygon.osm_ids))
      AND funicular = 'yes'
      AND subclass = 'station';

    UPDATE osm_poi_polygon
    SET tags = update_tags(tags, geometry)
    WHERE (full_update OR osm_id IN (SELECT osm_id FROM poi_polygon.osm_ids))
      AND COALESCE(tags->'name:latin', tags->'name:nonlatin', tags->'name_int') IS NULL
      AND tags != update_tags(tags, geometry);

$$ LANGUAGE SQL;

SELECT update_poi_polygon(true);

-- Handle updates

CREATE OR REPLACE FUNCTION poi_polygon.store() RETURNS trigger AS
$$
BEGIN
    IF (tg_op = 'DELETE') THEN
        INSERT INTO poi_polygon.osm_ids VALUES (OLD.osm_id);
    ELSE
        INSERT INTO poi_polygon.osm_ids VALUES (NEW.osm_id);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS poi_polygon.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION poi_polygon.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO poi_polygon.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION poi_polygon.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh poi_polygon';
    PERFORM update_poi_polygon(false);
    -- noinspection SqlWithoutWhere
    DELETE FROM poi_polygon.osm_ids;
    -- noinspection SqlWithoutWhere
    DELETE FROM poi_polygon.updates;

    RAISE LOG 'Refresh poi_polygon done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_store
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_poi_polygon
    FOR EACH ROW
EXECUTE PROCEDURE poi_polygon.store();

CREATE TRIGGER trigger_flag
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_poi_polygon
    FOR EACH STATEMENT
EXECUTE PROCEDURE poi_polygon.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON poi_polygon.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE poi_polygon.refresh();

-- Layer poi - ./update_poi_point.sql

DROP TRIGGER IF EXISTS trigger_flag ON osm_poi_point;
DROP TRIGGER IF EXISTS trigger_refresh ON poi_point.updates;

-- etldoc:  osm_poi_point ->  osm_poi_point
CREATE OR REPLACE FUNCTION update_osm_poi_point() RETURNS void AS
$$
BEGIN
    UPDATE osm_poi_point
    SET subclass = 'subway'
    WHERE station = 'subway'
      AND subclass = 'station';

    UPDATE osm_poi_point
    SET subclass = 'halt'
    WHERE funicular = 'yes'
      AND subclass = 'station';

    UPDATE osm_poi_point
    SET tags = update_tags(tags, geometry)
    WHERE COALESCE(tags->'name:latin', tags->'name:nonlatin', tags->'name_int') IS NULL;

END;
$$ LANGUAGE plpgsql;

SELECT update_osm_poi_point();

CREATE OR REPLACE FUNCTION update_osm_poi_point_agg() RETURNS void AS
$$
BEGIN
    UPDATE osm_poi_point p
    SET agg_stop = CASE
                       WHEN p.subclass IN ('bus_stop', 'bus_station', 'tram_stop', 'subway')
                           THEN 1
        END;

    UPDATE osm_poi_point p
    SET agg_stop = (
        CASE
            WHEN p.subclass IN ('bus_stop', 'bus_station', 'tram_stop', 'subway')
                     AND r.rk IS NULL OR r.rk = 1
                THEN 1
            END)
    FROM osm_poi_stop_rank r
    WHERE p.osm_id = r.osm_id;

END;
$$ LANGUAGE plpgsql;

ALTER TABLE osm_poi_point
    ADD COLUMN IF NOT EXISTS agg_stop integer DEFAULT NULL;
SELECT update_osm_poi_point_agg();

-- Handle updates

CREATE SCHEMA IF NOT EXISTS poi_point;

CREATE TABLE IF NOT EXISTS poi_point.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION poi_point.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO poi_point.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION poi_point.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh poi_point';
    PERFORM update_osm_poi_point();
    REFRESH MATERIALIZED VIEW osm_poi_stop_centroid;
    REFRESH MATERIALIZED VIEW osm_poi_stop_rank;
    PERFORM update_osm_poi_point_agg();
    -- noinspection SqlWithoutWhere
    DELETE FROM poi_point.updates;

    RAISE LOG 'Refresh poi_point done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_flag
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_poi_point
    FOR EACH STATEMENT
EXECUTE PROCEDURE poi_point.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON poi_point.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE poi_point.refresh();

-- Layer poi - ./poi.sql

-- etldoc: layer_poi[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="layer_poi | <z12> z12 | <z13> z13 | <z14_> z14+" ] ;

CREATE OR REPLACE FUNCTION layer_poi(bbox geometry, zoom_level integer, pixel_width numeric)
    RETURNS TABLE
            (
                osm_id   bigint,
                geometry geometry,
                name     text,
                name_en  text,
                name_de  text,
                tags     hstore,
                class    text,
                subclass text,
                agg_stop integer,
                layer    integer,
                level    integer,
                indoor   integer,
                "rank"   int
            )
AS
$$
SELECT osm_id_hash AS osm_id,
       geometry,
       NULLIF(name, '') AS name,
       COALESCE(NULLIF(name_en, ''), name) AS name_en,
       COALESCE(NULLIF(name_de, ''), name, name_en) AS name_de,
       tags,
       poi_class(subclass, mapping_key) AS class,
       CASE
           WHEN subclass = 'information'
               THEN NULLIF(information, '')
           WHEN subclass = 'place_of_worship'
               THEN NULLIF(religion, '')
           WHEN subclass = 'pitch'
               THEN NULLIF(sport, '')
           ELSE subclass
           END AS subclass,
       agg_stop,
       NULLIF(layer, 0) AS layer,
       "level",
       CASE WHEN indoor = TRUE THEN 1 END AS indoor,
       row_number() OVER (
           PARTITION BY LabelGrid(geometry, 100 * pixel_width)
           ORDER BY CASE WHEN name = '' THEN 2000 ELSE poi_class_rank(poi_class(subclass, mapping_key)) END ASC
           )::int AS "rank"
FROM (
         -- etldoc: osm_poi_point ->  layer_poi:z12
         -- etldoc: osm_poi_point ->  layer_poi:z13
         SELECT *,
                osm_id * 10 AS osm_id_hash
         FROM osm_poi_point
         WHERE geometry && bbox
           AND zoom_level BETWEEN 12 AND 13
           AND ((subclass = 'station' AND mapping_key = 'railway')
             OR subclass IN ('halt', 'ferry_terminal'))

         UNION ALL

         -- etldoc: osm_poi_point ->  layer_poi:z14_
         SELECT *,
                osm_id * 10 AS osm_id_hash
         FROM osm_poi_point
         WHERE geometry && bbox
           AND zoom_level >= 14

         UNION ALL

         -- etldoc: osm_poi_polygon ->  layer_poi:z12
         -- etldoc: osm_poi_polygon ->  layer_poi:z13
         SELECT *,
                NULL::integer AS agg_stop,
                CASE
                    WHEN osm_id < 0 THEN -osm_id * 10 + 4
                    ELSE osm_id * 10 + 1
                    END AS osm_id_hash
         FROM osm_poi_polygon
         WHERE geometry && bbox
           AND zoom_level BETWEEN 12 AND 13
           AND ((subclass = 'station' AND mapping_key = 'railway')
             OR subclass IN ('halt', 'ferry_terminal'))

         UNION ALL

         -- etldoc: osm_poi_polygon ->  layer_poi:z14_
         SELECT *,
                NULL::integer AS agg_stop,
                CASE
                    WHEN osm_id < 0 THEN -osm_id * 10 + 4
                    ELSE osm_id * 10 + 1
                    END AS osm_id_hash
         FROM osm_poi_polygon
         WHERE geometry && bbox
           AND zoom_level >= 14
     ) AS poi_union
ORDER BY "rank"
$$ LANGUAGE SQL STABLE
                PARALLEL SAFE;
-- TODO: Check if the above can be made STRICT -- i.e. if pixel_width could be NULL

DO $$ BEGIN RAISE NOTICE 'Finished layer poi'; END$$;
