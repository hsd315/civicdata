attach '/Volumes/DataLibrary/library/local/census.gov/2000_population-geo-orig-a7d9-r1.db' as a;
attach '/Volumes/DataLibrary/library/local/census.gov/2010_population-geo-orig-a7d9-r1.db' as b


INSERT INTO regions SELECT region, lsadc, trim(name) as name from a.region
