--
-- Help with Joins: http://www.codinghorror.com/blog/2007/10/a-visual-explanation-of-sql-joins.html

attach 'sf1.db' as sf1;
attach 'sf2.db' as sf2;
attach 'sf3.db' as sf3;
attach 'sf4.db' as sf4;
attach 'sumlev.db' as sl;

select "------";
--SELECT DISTINCT sumlev FROM sf3.geofile EXCEPT SELECT DISTINCT sumlev FROM sf4.geofile;
select "------";
--SELECT DISTINCT sumlev FROM sf4.geofile EXCEPT SELECT DISTINCT sumlev FROM sf3.geofile;
select "------";
--SELECT DISTINCT sumlev FROM sf4.geofile INTERSECT SELECT DISTINCT sumlev FROM sf3.geofile;

--SELECT count(DISTINCT sumlev) FROM sf3.geofile;
--SELECT count(DISTINCT sumlev) FROM sf4.geofile;

select "------";
--EXPLAIN QUERY PLAN 
--SELECT sf3.geofile.sumlev, sf3.geofile.geocomp, sf3.geofile.chariter, sf3.geofile.name FROM sf3.geofile 
--LEFT JOIN sf4.geofile ON sf3.geofile.logrecno = sf4.geofile.logrecno and sf3.geofile.stusab = sf4.geofile.stusab
--WHERE sf4.geofile.logrecno IS NOT NULL AND sf3.geofile.sumlev != 85 AND sf3.geofile.sumlev != 90
--;

