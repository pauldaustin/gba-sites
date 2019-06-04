#!/bin/bash
cd ../oracle
for file in extra/* tables/*; do 
  echo $file
  mkdir -p ../postgis/`dirname $file`
  oracleToPostgresql.sh $file ../postgis GBA
done

cd ../postgis
sed -i "" "s/MDSYS.SDO_GEOMETRY/geometry(MULTIPOLYGON,3005)/g" tables/INTEGRATION_SESSION_POLY.sql
sed -i "" "s/MDSYS.SDO_GEOMETRY/geometry(MULTIPOLYGON,3005)/g" tables/LOCALITY_POLY.sql
sed -i "" "s/MDSYS.SDO_GEOMETRY/geometry(MULTIPOLYGON,3005)/g" tables/COMMUNITY_POLY.sql
sed -i "" "s/MDSYS.SDO_GEOMETRY/geometry(POLYGON,3005)/g" tables/REGIONAL_DISTRICT_POLY.sql
sed -i "" "s/MDSYS.SDO_GEOMETRY/geometry(LINESTRING,3005)/g" tables/TRANSPORT_LINE.sql
sed -i "" "s/MDSYS.SDO_GEOMETRY/geometry(POINT,3005)/g" tables/TRANSPORT_LINE_NODE_POINT.sql
sed -i "" "s/MDSYS.SDO_GEOMETRY/geometry(POINT,3005)/g" tables/SITE_POINT.sql

