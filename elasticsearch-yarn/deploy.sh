mvn clean install
hadoop fs -rm elasticsearch/elasticsearch-yarn-1.0.0-SNAPSHOT.jar
hadoop fs -put target/elasticsearch-yarn-1.0.0-SNAPSHOT.jar elasticsearch/
