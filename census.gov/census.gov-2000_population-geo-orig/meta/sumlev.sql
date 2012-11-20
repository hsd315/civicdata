--
-- Help with Joins: http://www.codinghorror.com/blog/2007/10/a-visual-explanation-of-sql-joins.html

attach 'sf1.db' as sf1;
attach 'sf2.db' as sf2;
attach 'sf3.db' as sf3;
attach 'sf4.db' as sf4;
attach 'sumlev.db' as sl;

DROP TABLE IF EXISTS sl.sumlev;
CREATE TABLE sl.sumlev AS 
SELECT DISTINCT trim(sumlev) as sumlev, fileid FROM (
SELECT DISTINCT sumlev, fileid  FROM sf1.geofile
UNION
SELECT DISTINCT sumlev, fileid  FROM sf2.geofile
UNION
SELECT DISTINCT sumlev, fileid  FROM sf3.geofile
UNION
SELECT DISTINCT sumlev, fileid  FROM sf4.geofile
);

DROP TABLE IF EXISTS sl.slfiles;

CREATE TABLE sl.slfiles AS 
SELECT DISTINCT  s0.sumlev, s1.fileid as sf1_file, s2.fileid as sf2_file, s3.fileid as sf3_file, s4.fileid as sf4_file
FROM sl.sumlev as s0
LEFT JOIN sl.sumlev s1 ON s1.sumlev = s0.sumlev AND s1.fileid = 'uSF1  '
LEFT JOIN sl.sumlev s2 ON s2.sumlev = s0.sumlev AND s2.fileid = 'uSF2  '
LEFT JOIN sl.sumlev s3 ON s3.sumlev = s0.sumlev AND s3.fileid = 'uSF3  '
LEFT JOIN sl.sumlev s4 ON s4.sumlev = s0.sumlev AND s4.fileid = 'uSF4  '
;
