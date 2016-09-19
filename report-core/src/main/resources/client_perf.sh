#!/bin/bash
ClientPerf_JAR=sunflower-bi-sdk-0.2.0-SNAPSHOT-jar-with-dependencies.jar
ClientPerf_MAIN=com.maplecloudy.thrift.hqlquery.HiveQueryServer

# Add hadoop-lib
if test -z "$HADOOP_HOME"
then
	HadoopHomeDir=/usr/lib/hadoop
else
	HadoopHomeDir=$HADOOP_HOME
fi

# Add hadoop-config
if test -z "$HADOOP_CONF_DIR"
then
	HadoopConfigDir=$HadoopHomeDir/conf
else
	HadoopConfigDir=$HADOOP_CONF_DIR
fi

# Add hive-lib
if test -z "$HIVE_HOME"
then
	HiveHomeDir=/usr/lib/hive
else
	HiveHomeDir=$HIVE_HOME
fi

# Add hive-config
if test -z "$HIVE_CONF_DIR"
then
	HiveConfigDir=/etc/hive/conf
else
	HiveConfigDir=$HIVE_CONF_DIR
fi

CLASSPATH=.

DatanucleusJarDir=`pwd`/lib
for f in $DatanucleusJarDir/*.jar
do
	CLASSPATH=$CLASSPATH:$f
done

CLASSPATH=$CLASSPATH:$HadoopConfigDir
CLASSPATH=$CLASSPATH:$HiveConfigDir

echo $CLASSPATH
goalJar=`pwd`/$ClientPerf_JAR

java -Dfile.encoding=UTF-8 -cp $CLASSPATH:$goalJar $ClientPerf_MAIN
