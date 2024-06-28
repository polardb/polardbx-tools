#!/bin/sh
version=$1
mvn clean
mvn assembly:assembly
cd target
rm -rf mysqlsniffer-${version}
mkdir mysqlsniffer-${version}
mkdir -p mysqlsniffer-${version}/logs
cp ../README.md mysqlsniffer-${version}/
cp ../app.properties mysqlsniffer-${version}/
cp mysqlsniffer-*.jar mysqlsniffer-${version}/mysqlsniffer.jar
cp -r ../lib mysqlsniffer-${version}/

rm -f  mysqlsniffer-${version}.tar.gz
tar zcvf mysqlsniffer-${version}.tar.gz mysqlsniffer-${version}
cd ../






