#!/bin/sh
version="1.1.30"
cd mysqlsniffer
sh build.sh $version
cd ../

cd slssniffer
sh build.sh $version
cd ../

cd frodo-core
sh build.sh $version
cd ../
rm -rf target
mkdir target
cp frodo-core/target/frodo-${version}.tar.gz ./target





