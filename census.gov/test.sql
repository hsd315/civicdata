attach database '2000.db' as g2000;
attach database '2010.db' as g2010;
attach database 'census.gov-ACS-geo-orig/build/census.gov/acs-geo-orig-a7d9-r1/geofile/20105.db' as g20105;


select g1.place, g1.name, g2.name, g3.name
from 
g2000.places as g1,
g2010.places as g2,
g20105.places as g3
where g1.geoid = g2.geoid and g1.geoid = g3.geoid
limit 100
;

