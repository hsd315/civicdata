create temp view sf1geo2000 as
select *
from area,grain,block,county,metro_type,place,
record_code,state,urban_type
where area.area_id = record_code.area_id
AND block.block_id = record_code.block_id
AND grain.grain_id = record_code.grain_id
AND county.county_id = record_code.county_id
AND metro_type.metro_type_id = record_code.metro_type_id
AND place.place_id = record_code.place_id
AND record_code.record_code_id = record_code.record_code_id
AND state.state_id = record_code.state_id
AND urban_type.urban_type_id = record_code.urban_type_id;

CREATE INDEX area_i1 ON area (arealand);
CREATE INDEX area_i2 ON area (areawatr);
CREATE INDEX area_i3 ON area (intptlat, intptlon);

CREATE INDEX county_i1 ON county (county);
CREATE INDEX county_i2 ON county (countysc);

CREATE INDEX grain_i1 ON grain (sumlev);
CREATE INDEX grain_i2 ON grain (chariter);

CREATE INDEX leg_district_i1 ON leg_district (sldu, sldl);

CREATE INDEX place_i1 ON place (place);
CREATE INDEX place_i2 ON place (placecc);

CREATE INDEX record_code_i1 ON record_code (stusab);

CREATE UNIQUE INDEX record_code_ui1 ON record_code (stusab, logrecno);