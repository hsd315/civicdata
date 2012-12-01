--
-- Help with Joins: http://www.codinghorror.com/blog/2007/10/a-visual-explanation-of-sql-joins.html

attach 'all.db' as agf;
attach 'sumlev.db' as sl;

DROP TABLE IF EXISTS sl.sumlev;

CREATE TABLE sl.sumlev AS 
SELECT DISTINCT trim(sumlev) as sumlev, fileid FROM agf.geofile;

DROP TABLE IF EXISTS sl.slfiles;

CREATE TABLE sl.slfiles AS 
SELECT DISTINCT  s0.sumlev, 
s1.fileid as sf1_file, s2.fileid as sf2_file, s3.fileid as sf3_file, s4.fileid as sf4_file,
s1us.fileid as sf1us_file, s2us.fileid as sf2us_file, s3us.fileid as sf3us_file, s4us.fileid as sf4us_file
FROM sl.sumlev as s0
LEFT JOIN sl.sumlev s1 ON s1.sumlev = s0.sumlev AND trim(s1.fileid) = 'uSF1'
LEFT JOIN sl.sumlev s2 ON s2.sumlev = s0.sumlev AND trim(s2.fileid) = 'uSF2'
LEFT JOIN sl.sumlev s3 ON s3.sumlev = s0.sumlev AND trim(s3.fileid) = 'uSF3'
LEFT JOIN sl.sumlev s4 ON s4.sumlev = s0.sumlev AND trim(s4.fileid) = 'uSF4'
LEFT JOIN sl.sumlev s1us ON s1.sumlev = s0.sumlev AND trim(s1us.fileid) = 'uSF1F'
LEFT JOIN sl.sumlev s2us ON s2.sumlev = s0.sumlev AND trim(s2us.fileid) = 'uSF2F'
LEFT JOIN sl.sumlev s3us ON s3.sumlev = s0.sumlev AND trim(s3us.fileid) = 'uSF3F'
LEFT JOIN sl.sumlev s4us ON s4.sumlev = s0.sumlev AND trim(s4us.fileid) = 'uSF4F'
;

DROP TABLE IF EXISTS sl.slcounts;
CREATE TABLE sl.slcounts AS
select cast(trim(sumlev) as INTEGER) AS sumlev, trim(fileid), count(*) as count from geofile group by sumlev, fileid;

#
# determine if each summary level includes greographies that are split
# across higher level geographies. 
#
DROP TABLE IF EXISTS sl.partlevs;
CREATE TABLE  sl.partlevs AS
select distinct cast(trim(sumlev) as INTEGER) AS sumlev, 'N' from geofile where trim(partflag) = '' 
UNION select distinct cast(trim(sumlev) as INTEGER), 'Y' from geofile where trim(partflag) != '';


SELECT DISTINCT  s0.sumlev, 
s1.fileid as sf1_file, s2.fileid as sf2_file, s3.fileid as sf3_file, s4.fileid as sf4_file,
s1us.fileid as sf1us_file, s2us.fileid as sf2us_file, s3us.fileid as sf3us_file, s4us.fileid as sf4us_file
FROM sumlev as s0
LEFT JOIN sumlev s1 ON s1.sumlev = s0.sumlev AND s1.fileid = 'uSF1'
LEFT JOIN sumlev s2 ON s2.sumlev = s0.sumlev AND s2.fileid = 'uSF2'
LEFT JOIN sumlev s3 ON s3.sumlev = s0.sumlev AND s3.fileid = 'uSF3'
LEFT JOIN sumlev s4 ON s4.sumlev = s0.sumlev AND s4.fileid = 'uSF4'
LEFT JOIN sumlev s1us ON s1.sumlev = s0.sumlev AND s1us.fileid = 'uSF1F'
LEFT JOIN sumlev s2us ON s2.sumlev = s0.sumlev AND s2us.fileid = 'uSF2F'
LEFT JOIN sumlev s3us ON s3.sumlev = s0.sumlev AND s3us.fileid = 'uSF3F'
LEFT JOIN sumlev s4us ON s4.sumlev = s0.sumlev AND s4us.fileid = 'uSF4F'
;
