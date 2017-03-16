apache-maven-3.3.9.zip/apache-maven-3.3.9/bin/mvn clean package -f sixddefense.zip/sixddefense/pom.xml
echo "start deploy the project....."
hadoop fs -copyFromLocal -f sixddefense.zip/sixddefense/sixddfense-dist/target/sixddefense-dist-1.0.0-SNAPSHOT-bin sixddefense-dist-1.0.0-SNAPSHOT-bin
echo "succes to deploy the project....."
