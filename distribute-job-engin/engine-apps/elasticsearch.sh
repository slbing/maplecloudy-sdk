echo `whoami` 
chmod 777 -R elasticsearch-5.3.0.zip/elasticsearch-5.3.0/*
echo "changed permission to 777"
rm elasticsearch-5.3.0.zip/elasticsearch-5.3.0/config/elasticsearch.yml
cp elasticsearch.yml elasticsearch-5.3.0.zip/elasticsearch-5.3.0/config
rm elasticsearch-5.3.0.zip/elasticsearch-5.3.0/config/jvm.options
cp jvm.options elasticsearch-5.3.0.zip/elasticsearch-5.3.0/config
export JAVA_HOME="jdk-8u121-linux-x64.tar.gz/jdk1.8.0_121"
echo "jdk1.8 exported"
elasticsearch-5.3.0.zip/elasticsearch-5.3.0/bin/elasticsearch