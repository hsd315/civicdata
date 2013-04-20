Processed crime incidents, based on data supplied by SANDAG. (more)

This bundle includes geocoded crime incidents from 1 Jan 2007 to 31 March 2013 that were returned by SANDAG for Public Records request 12-075. The bundle does not include all of the incidents returned in the request. It is missing incidents where:

  * The address could not be geocoded
  * The address is represented as an intersection of two streets

The files that SANDAG returned included 1,008,524 incident records, while the geocoded data presented here includes 655,635 records.

Most users will want to use the CSV version of the data. This file includes several columns that are not in the sqlite database. The columns in the CSV file are:

  * date		ISO date, in YY-MM-DD format
  * month		Month number extracted from the date
  * week		ISO week of the year
  * dow			Day of week, as a number. 0 is Sunday
  * time		Time, in H:MM:SS format
  * hour		Hour number, extracted from the time
  * legend		Crime category, provided by SANDAG *This is the short crime type*
  * number		House number used for geocoding
  * street		Name of street, based on SANDAG provided block address
  * city		City. Some city records are missing
  * state		State. Always "CA"
  * zip			Zip code. Some are missing
  * lat			Latitude, provided by the geocoding service. 
  * lon			Longitude, provided by the geocoding service. 
  * description	Long description of incident. 

Addresses and Cleaning
=======================
SANDAG returns the position of incidents as a block address, and occasionally as an intersection. Block addresses are the original address of the incident, with the last two digits set to '00'. 

Before geocoding, all of the original block addresses are normalized to be more consistent and to remove different versions of the same address. There are a few transformations performed on the address, including:

  * Converting street types synonyms like 'Avenue', 'Avenu' and 'ave.' to standard abbreviations like 'ave.'
  * Converting street directions ( 'West main Street' ) to abbreviations like 'W Main st'

Intersection addresses are composed of two street names. Incidents with intersection addresses are excluded. 
  
There are many other conversions; see the code for details. 

Geocoding
=========

Many geocoders are designed to work with mailable addresses, and block addresses are not real postal addresses. So, when the block addresses is geocoded, our programs increment the number until it geocodes properly. The result is that the points represented by the (lat,lon) values are not at intersections, as you would get if you geocoded the block address. 

Using block addresses also means that all of the incidents on a block will appear at a single location. In most GIS programs, it is difficult to see that there are actually many points in one place. Be aware that each point you see may actually be dozens of incidents. 

Dataset Statistics
==================

Number of incidents by year:

  * 2007	166359
  * 2008	156033
  * 2009	140428
  * 2010	136135
  * 2011	126047
  * 2012	122979
  * 2013	27789

Crime types, from the "legend" field, and the number of that type

  * 184775	DRUGS/ALCOHOL VIOLATIONS
  * 129008	THEFT/LARCENY
  * 111750	VEHICLE BREAK-IN/THEFT
  * 86507	MOTOR VEHICLE THEFT
  * 84930	BURGLARY
  * 75923	VANDALISM
  * 63859	ASSAULT
  * 49688	FRAUD
  * 38921	DUI
  * 20100	ROBBERY
  * 19005	SEX CRIMES
  * 9010	WEAPONS
  * 1823	ARSON
  * 471	HOMICIDE



