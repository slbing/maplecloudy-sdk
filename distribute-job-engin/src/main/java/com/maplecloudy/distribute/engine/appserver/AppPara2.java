package com.maplecloudy.distribute.engine.appserver;

import org.apache.hadoop.conf.Configuration;

public class AppPara2 {
  
  public String user = "maplecloudy";
  
  public String project = "";
  
  public String appConf = "";
  public int[] appId = {0};
  public int memory = 1024;
  public int cpu = 1;
  
  //nginx para
  public String domain;
  public String nginxIp ;
  public String nginxDomain;
  public int port ;
//  @Nullable
//  public NgixGateway gateway;
//  public Cluster cluster;
  public String defaultFS ="";
  public String resourceManagerAddress = "";

  
  public boolean isDistribution = true;
  
  
  public void setConf(Configuration conf )
  {
    conf.set("yarn.resourcemanager.address",this.resourceManagerAddress);
    conf.set("fs.defaultFS",this.defaultFS);
    
    conf.set("yarn.application.classpath", "$HADOOP_CLIENT_CONF_DIR,$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*");
  }

}
