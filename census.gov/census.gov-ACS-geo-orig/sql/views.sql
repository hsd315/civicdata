-- 20105 ACS


DROP VIEW IF EXISTS states;
CREATE VIEW states AS
SELECT DISTINCT geoid, state,  trim(name) as name
FROM geofile WHERE sumlevel =  40   and component = '00' ;

DROP VIEW IF EXISTS counties;
CREATE VIEW counties AS
SELECT DISTINCT geoid, state,  county, trim(name) as name
FROM geofile WHERE sumlevel =  50   and component = '00' ;

DROP VIEW IF EXISTS cosubs;
CREATE VIEW cosubs AS
SELECT DISTINCT geoid, state, county, cousub,trim(name) as name
FROM geofile WHERE sumlevel =  60   and component = '00' ;

DROP VIEW IF EXISTS tracts;
CREATE VIEW tracts AS
SELECT DISTINCT  geoid, state, county, tract,  trim(name) as name
FROM geofile WHERE sumlevel =  140  and component = '00' ;

DROP VIEW IF EXISTS blockgroups;
CREATE VIEW blockgroups AS
SELECT DISTINCT geoid, state,  county,  tract, blkgrp, trim(name) as name
FROM geofile WHERE sumlevel =  150  and component = '00' ;

DROP VIEW IF EXISTS csas;
CREATE VIEW csas AS
SELECT DISTINCT geoid, state, csa, trim(name) as name
FROM geofile WHERE sumlevel =  330  and component = '00';

DROP VIEW IF EXISTS cbsas;
CREATE VIEW cbsas AS
SELECT DISTINCT geoid, state, cbsa, trim(name) as name
FROM geofile WHERE sumlevel =  310  and component = '00';

DROP VIEW IF EXISTS places;
CREATE VIEW places AS
SELECT DISTINCT geoid, state,  place, trim(name) as name
FROM geofile WHERE sumlevel =  160  and component = '00' ;

