mvn clean install
hadoop fs -rm elasticsearch/elasticsearch-yarn-0.3.0-SNAPSHOT.jar
hadoop fs -put -f target/elasticsearch-yarn-0.3.0-SNAPSHOT.jar elasticsearch/
