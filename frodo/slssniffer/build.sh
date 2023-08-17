#!/bin/sh
version=$1
mvn assembly:assembly
cd target
tar zcvf slssniffer-${version}.tar.gz slssniffer-*.jar
