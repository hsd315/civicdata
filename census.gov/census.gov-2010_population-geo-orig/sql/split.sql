attach 'build/census.gov/2010_population-geo-orig-a7d9-r1/geofile/all.db' as agf;

select "Regions";
DROP TABLE IF EXISTS regions;
CREATE TABLE regions AS
SELECT DISTINCT region,
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  20 and chariter = 0 and geocomp = '00' ;

select "Divisions";
DROP TABLE IF EXISTS divisions;
CREATE TABLE divisions AS
SELECT DISTINCT region, division, 
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  30  and chariter = 0 and geocomp = '00' ;

select "States";
DROP TABLE IF EXISTS states;
CREATE TABLE states AS
SELECT DISTINCT region, division, state, 
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  40  and chariter = 0 and geocomp = '00' ;

select "Counties";
DROP TABLE IF EXISTS counties;
CREATE TABLE counties AS
SELECT DISTINCT region, division, state, county, 
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  50  and chariter = 0 and geocomp = '00' ;

select "County Subdivisions";
DROP TABLE IF EXISTS cosubs;
CREATE TABLE cosubs AS
SELECT DISTINCT region, division, state, county, countysc, cousub, cousubcc, cousubsc, 
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  60  and chariter = 0 and geocomp = '00' ;

select "Tracts";
DROP TABLE IF EXISTS tracts;
CREATE TABLE tracts AS
SELECT DISTINCT region, division, state, county, countysc, tract, 
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  140 and chariter = 0 and geocomp = '00' ;

select "Blockgroups";
DROP TABLE IF EXISTS blockgroups;
CREATE TABLE blockgroups AS
SELECT DISTINCT region, division, state, county, countysc, tract, blkgrp,
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  150 and chariter = 0 and geocomp = '00' ;

select "Places";
DROP TABLE IF EXISTS places;
CREATE TABLE places AS
SELECT DISTINCT region, division, state, place, placecc, placedc, placesc,exi, msacmsa,masc,cmsa,macci,pmsa
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  160 and chariter = 0 and geocomp = '00' ;

select "Metros";
DROP TABLE IF EXISTS metros;
CREATE TABLE metros AS
SELECT DISTINCT msacmsa,masc,cmsa, 
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  380 and chariter = 0 and geocomp = '00' ;

select "Urban";
DROP TABLE IF EXISTS urban;
CREATE TABLE metros AS
SELECT DISTINCT msacmsa,masc,cmsa, 
arealand, areawatr, pop100, hu100, intptlat, intptlon, gcuni,lsadc, trim(name) as name
FROM geofile WHERE sumlev =  400 and chariter = 0 and geocomp = 0 ;

attach 'areas.db' as areas;
select "Areas";
DROP TABLE IF EXISTS areas.areas;
CREATE TABLE areas.areas AS
SELECT DISTINCT geocomp, arealand, areawatr, pop100, hu100 FROM geofile;
detatch areas;


