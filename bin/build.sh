#!/usr/bin/env bash

#echo ${M2_HOME}
#export

#source /etc/profile

#echo "================"

#export

#echo "#"

bin=$(dirname $0)
cd $bin
cd ..
mvn clean install

