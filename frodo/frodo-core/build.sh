#!/bin/sh
version=$1
mvn clean
mvn assembly:assembly
cd target
rm -rf frodo-${version}
mkdir frodo-${version}
mkdir -p frodo-${version}/logs

cp ../README.md frodo-${version}/
cp ../rds_audit_transfer.py frodo-${version}/
cp ../polarx_audit_transfer.py frodo-${version}/
cp ../polarx_cn_transfer.py frodo-${version}/
cp ../polarx_cn_transfer_parallel.sh frodo-${version}/
cp frodo-*.jar frodo-${version}/frodo.jar
mkdir -p frodo-${version}/collector
#cp -r ../collector frodo/
cp -r ../../slssniffer/target/slssniffer*.tar.gz frodo-${version}/collector
cp -r ../../mysqlsniffer/target/mysqlsniffer*.tar.gz frodo-${version}/collector
rm -f  frodo-${version}.tar.gz
tar zcvf frodo-${version}.tar.gz frodo-${version}
cd ../






