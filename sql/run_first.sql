-- This SQL code should be executed first

CREATE OR REPLACE FUNCTION slice_language_tags(tags hstore)
RETURNS hstore AS $$
    SELECT delete_empty_keys(slice(tags, ARRAY['name:am', 'name:ar', 'name:az', 'name:be', 'name:bg', 'name:br', 'name:bs', 'name:ca', 'name:co', 'name:cs', 'name:cy', 'name:da', 'name:de', 'name:el', 'name:en', 'name:eo', 'name:es', 'name:et', 'name:eu', 'name:fi', 'name:fr', 'name:fy', 'name:ga', 'name:gd', 'name:he', 'name:hi', 'name:hr', 'name:hu', 'name:hy', 'name:id', 'name:is', 'name:it', 'name:ja', 'name:ja_kana', 'name:ja_rm', 'name:ja-Latn', 'name:ja-Hira', 'name:ka', 'name:kk', 'name:kn', 'name:ko', 'name:ko-Latn', 'name:ku', 'name:la', 'name:lb', 'name:lt', 'name:lv', 'name:mk', 'name:mt', 'name:ml', 'name:nl', 'name:no', 'name:oc', 'name:pl', 'name:pt', 'name:rm', 'name:ro', 'name:ru', 'name:sk', 'name:sl', 'name:sq', 'name:sr', 'name:sr-Latn', 'name:sv', 'name:ta', 'name:te', 'name:th', 'name:tr', 'name:uk', 'name:zh', 'int_name', 'loc_name', 'name', 'wikidata', 'wikipedia']))
$$ LANGUAGE SQL IMMUTABLE;
