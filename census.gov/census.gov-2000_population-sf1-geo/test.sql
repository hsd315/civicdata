attach 'build/census.gov/2000_population-sf1-geo-a7d9-r1/record_code.db' as rc;
attach 'build/census.gov/2000_population-sf1-geo-a7d9-r1/tempmap.db' as tm;

PRAGMA cache_size = -2000000; -- use up to 2G of memory

SELECT 
NULL, rc.record_code.fileid,rc.record_code.stusab,rc.record_code.sumlev,rc.record_code.geocomp,rc.record_code.chariter,rc.record_code.cifsn,rc.record_code.logrecno,rc.record_code.geoid, 
tm.release_map_htmap.id as release_id,
tm.grain_map_htmap.id as grain_id,
tm.name_map_htmap.id as name_id,
tm.area_map_htmap.id as area_id,
tm.phcount_map_htmap.id as phcount_id,
tm.location_map_htmap.id as location_id,
tm.flags_map_htmap.id as flags_id,
tm.zacta_map_htmap.id as zacta_id,
tm.state_map_htmap.id as state_id,
tm.division_map_htmap.id as division_id,
tm.block_map_htmap.id as block_id,
tm.blockgroup_map_htmap.id as blockgroup_id,
tm.tract_map_htmap.id as tract_id,
tm.taz_map_htmap.id as taz_id,
tm.puma_map_htmap.id as puma_id,
tm.county_map_htmap.id as county_id,
tm.leg_district_map_htmap.id as leg_district_id,
tm.metro_type_map_htmap.id as metro_type_id,
tm.place_map_htmap.id as place_id,
tm.schools_map_htmap.id as schools_id,
tm.spec_area_map_htmap.id as spec_area_id,
tm.urban_type_map_htmap.id as urban_type_id
FROM rc.record_code
 JOIN tm.release_map_htmap ON tm.release_map_htmap.hash = rc.record_code.release_id
 JOIN tm.grain_map_htmap ON tm.grain_map_htmap.hash = rc.record_code.grain_id
 JOIN tm.name_map_htmap ON tm.name_map_htmap.hash = rc.record_code.name_id
 JOIN tm.area_map_htmap ON tm.area_map_htmap.hash = rc.record_code.area_id
 JOIN tm.phcount_map_htmap ON tm.phcount_map_htmap.hash = rc.record_code.phcount_id
 JOIN tm.location_map_htmap ON tm.location_map_htmap.hash = rc.record_code.location_id
 JOIN tm.flags_map_htmap ON tm.flags_map_htmap.hash = rc.record_code.flags_id
 JOIN tm.zacta_map_htmap ON tm.zacta_map_htmap.hash = rc.record_code.zacta_id
 JOIN tm.state_map_htmap ON tm.state_map_htmap.hash = rc.record_code.state_id
 JOIN tm.division_map_htmap ON tm.division_map_htmap.hash = rc.record_code.division_id
 JOIN tm.block_map_htmap ON tm.block_map_htmap.hash = rc.record_code.block_id
 JOIN tm.blockgroup_map_htmap ON tm.blockgroup_map_htmap.hash = rc.record_code.blockgroup_id
 JOIN tm.tract_map_htmap ON tm.tract_map_htmap.hash = rc.record_code.tract_id
 JOIN tm.taz_map_htmap ON tm.taz_map_htmap.hash = rc.record_code.taz_id
 JOIN tm.puma_map_htmap ON tm.puma_map_htmap.hash = rc.record_code.puma_id
 JOIN tm.county_map_htmap ON tm.county_map_htmap.hash = rc.record_code.county_id
 JOIN tm.leg_district_map_htmap ON tm.leg_district_map_htmap.hash = rc.record_code.leg_district_id
 JOIN tm.metro_type_map_htmap ON tm.metro_type_map_htmap.hash = rc.record_code.metro_type_id
 JOIN tm.place_map_htmap ON tm.place_map_htmap.hash = rc.record_code.place_id
 JOIN tm.schools_map_htmap ON tm.schools_map_htmap.hash = rc.record_code.schools_id
 JOIN tm.spec_area_map_htmap ON tm.spec_area_map_htmap.hash = rc.record_code.spec_area_id
 JOIN tm.urban_type_map_htmap ON tm.urban_type_map_htmap.hash = rc.record_code.urban_type_id
;




