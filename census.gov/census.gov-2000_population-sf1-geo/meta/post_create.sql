DROP VIEW IF EXISTS sf1geo2000;
CREATE VIEW sf1geo2000 as
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

CREATE INDEX IF NOT EXISTS grain_sumlev ON grain (sumlev);

CREATE INDEX IF NOT EXISTS state_region ON state (region);
CREATE INDEX IF NOT EXISTS state_division ON state (division);
CREATE INDEX IF NOT EXISTS state_state ON state (state);
CREATE INDEX IF NOT EXISTS state_statece ON state (statece);

CREATE INDEX IF NOT EXISTS  record_code_gain_id ON record_code (grain_id);