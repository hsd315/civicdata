clarinova.com-extents-2012-7ba4
===============================


Analysis Area Codes
-------------------

The analysis are codes are generally US Census geoids, but may other values for places that are not defined by the US Census. These codes are distinguished by the code prefix. 

	* CG: Census geoids
	* Other codes are reserved, to be used or legislative districts and neighborhoods. 


Rasterizing with GDAL
---------------------

Extract a feature to a shapefile: 

ogr2ogr -f "ESRI Shapefile" -where "COUNTYFP10 = '001'" ouput.shp input.shp 

Or, you can extract a set of features within a bounding box with: 

	 -spat xmin ymin xmax ymax

Rasterize it:

gdal_rasterize -ot Int16  -tr .001 .001  -where "COUNTYFP10 = '001'"  -a COUNTYFP10 -l county1 county1.shp county1.tiff

