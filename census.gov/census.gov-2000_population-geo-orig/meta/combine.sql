attach 'sf1.db' as sf1;
attach 'sf2.db' as sf2;
attach 'sf3.db' as sf3;
attach 'sf4.db' as sf4;
attach 'sf1us.db' as sf1us;
attach 'sf2us.db' as sf2us;
attach 'sf3us.db' as sf3us;
attach 'sf4us.db' as sf4us;

PRAGMA page_size = 8192;

attach 'all.db' as agf;

DROP TABLE IF EXISTS  agf.geofile; 
pragma agf.journal_mode = OFF;

select "------ 1/8";
CREATE TABLE agf.geofile AS SELECT *  FROM sf1.geofile;
select "------ 2/8";
INSERT INTO agf.geofile  SELECT *  FROM sf2.geofile;
select "------ 3/8";
INSERT INTO agf.geofile  SELECT *  FROM sf3.geofile;
select "------ 4/8";
INSERT INTO agf.geofile  SELECT *  FROM sf4.geofile;
select "------ 5/8";
INSERT INTO agf.geofile  SELECT *  FROM sf1us.geofile;
select "------ 6/8";
INSERT INTO agf.geofile  SELECT *  FROM sf2us.geofile;
select "------ 7/8";
INSERT INTO agf.geofile  SELECT *  FROM sf3us.geofile;
select "------ 8/8";
INSERT INTO agf.geofile  SELECT *  FROM sf4us.geofile;
select "------ Done ";

