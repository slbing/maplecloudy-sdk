chmod 777 -R jetty-distribution-7.6.21.v20160908.zip/jetty-distribution-7.6.21.v20160908/*
cp <war> jetty-distribution-7.6.21.v20160908.zip/jetty-distribution-7.6.21.v20160908/webapps/root.war
rm jetty-distribution-7.6.21.v20160908.zip/jetty-distribution-7.6.21.v20160908/etc/jetty.xml
cp jetty.xml jetty-distribution-7.6.21.v20160908.zip/jetty-distribution-7.6.21.v20160908/etc
rm jetty-distribution-7.6.21.v20160908.zip/jetty-distribution-7.6.21.v20160908/start.ini
cp jetty_start.ini jetty-distribution-7.6.21.v20160908.zip/jetty-distribution-7.6.21.v20160908/start.ini
jetty-distribution-7.6.21.v20160908.zip/jetty-distribution-7.6.21.v20160908/bin/jetty.sh start