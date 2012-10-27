
ATTACH "build/census.gov/2000_population-sf1-orig-a7d9-r1/record_code.db" as record_code;
ATTACH "build/census.gov/2000_population-sf1-orig-a7d9-r1/grain.db" as grain;
ATTACH "build/census.gov/2000_population-sf1-orig-a7d9-r1/area.db" as area;
ATTACH "build/census.gov/2000_population-sf1-orig-a7d9-r1/block.db" as block;
ATTACH "build/census.gov/2000_population-sf1-orig-a7d9-r1/county.db" as county;
ATTACH "build/census.gov/2000_population-sf1-orig-a7d9-r1/metro_type.db" as metro_type;
ATTACH "build/census.gov/2000_population-sf1-orig-a7d9-r1/place.db" as place;
ATTACH "build/census.gov/2000_population-sf1-orig-a7d9-r1/state.db" as state;
ATTACH "build/census.gov/2000_population-sf1-orig-a7d9-r1/urban_type.db" as urban_type;

PRAGMA record_code.table_info(record_code);
PRAGMA place.table_info(place);

create temp view all_rc as
select *
from area.area,grain.grain,block.block,county.county,metro_type.metro_type,place.place,
record_code.record_code,state.state,urban_type.urban_type
where area.area.area_id = record_code.record_code.area_id
AND block.block.block_id = record_code.record_code.block_id
AND grain.grain.grain_id = record_code.record_code.grain_id
AND county.county.county_id = record_code.record_code.county_id
AND metro_type.metro_type.metro_type_id = record_code.record_code.metro_type_id
AND place.place.place_id = record_code.record_code.place_id
AND record_code.record_code.record_code_id = record_code.record_code.record_code_id
AND state.state.state_id = record_code.record_code.state_id
AND urban_type.urban_type.urban_type_id = record_code.record_code.urban_type_id;
;


 -- select state, stusab, logrecno, sumlev, geocomp, name, pop100, hu100 from all_rc where sumlev = 91;
