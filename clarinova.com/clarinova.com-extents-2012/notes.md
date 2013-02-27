Implementation Notes
====================

Rasterizing with GDAL
---------------------

Extract a feature to a shapefile: 

ogr2ogr -f "ESRI Shapefile" -where "COUNTYFP10 = '001'" ouput.shp input.shp 

gdal_rasterize -ot Int16  -tr .001 .001  -where "COUNTYFP10 = '001'"  -a COUNTYFP10 -l county1 county1.shp county1.tiff

