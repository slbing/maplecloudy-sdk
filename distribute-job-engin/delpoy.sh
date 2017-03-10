mvn clean install
hadoop fs -rm distribute-job-engin-0.3.0-SNAPSHOT.jar
hadoop fs -put target/distribute-job-engin-0.3.0-SNAPSHOT.jar
