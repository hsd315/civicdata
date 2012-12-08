--- Census 2000 

DROP VIEW IF EXISTS states;
CREATE VIEW states AS
SELECT DISTINCT 
'04000US' || substr('00'||state,-2,2) as geoid,
region, division, state, 
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  40  and chariter = 0 and geocomp = 0 ;

DROP VIEW IF EXISTS counties;
CREATE VIEW counties AS
SELECT DISTINCT 
'05000US' || substr('00'||state,-2,2) || substr('000'||county,-3,3) as geoid,
region, division, state, statece, county, 
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  50  and chariter = 0 and geocomp = 0 ;

DROP VIEW IF EXISTS cosubs;
CREATE VIEW cosubs AS
SELECT DISTINCT 
'06000US' || substr('00'||state,-2,2) || substr('00'||county,-3,3) || substr('00000'||cousub,-5,5) as geoid,
region, division, state, statece, county, countysc, cousub, cousubcc, cousubsc, 
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  60  and chariter = 0 and geocomp = 0 ;

DROP VIEW IF EXISTS tracts;
CREATE VIEW tracts AS
SELECT DISTINCT 
'14000US' || substr('00'||state,-2,2) || substr('00'||county,-3,3) || substr('000000'||tract,-6,6) as geoid,
region, division, state, statece, county, countysc, tract, 
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  140 and chariter = 0 and geocomp = 0 ;

DROP VIEW IF EXISTS blockgroups;
CREATE VIEW blockgroups AS
SELECT DISTINCT 
'15000US' || substr('00'||state,-2,2) || substr('00'||county,-3,3) || substr('000000'||tract,-6,6) || blkgrp as geoid,
region, division, state, statece, county, countysc, tract, blkgrp,
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  150 and chariter = 0 and geocomp = 0 ;


DROP VIEW IF EXISTS places;
CREATE VIEW places AS
SELECT DISTINCT '16000US' || substr('00'||state,-2,2) || substr('00000'||place,-5,5) as geoid, 
region, division, state, state, statece, place, placecc, placedc, placesc,exi, msacmsa,masc,cmsa,macci,pmsa
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  160 and chariter = 0 and geocomp = 0 ;


DROP VIEW IF EXISTS metros;
CREATE VIEW metros AS
SELECT DISTINCT msacmsa,masc,cmsa, 
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  380 and chariter = 0 and geocomp = 0 ;

