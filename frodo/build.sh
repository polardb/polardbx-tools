#!/bin/sh
version="1.1.29"
cd mysqlsniffer
sh build.sh $version
cd ../

cd slssniffer
sh build.sh $version
cd ../

cd frodo
sh build.sh $version
cd ../
rm -rf target
mkdir target
cp frodo/target/frodo-${version}.tar.gz ./target





