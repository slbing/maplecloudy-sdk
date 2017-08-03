package com.maplecloudy.distribute.engine.appserver;

import org.apache.hadoop.conf.Configuration;

public abstract class AppPara {
  
  public String user = "maplecloudy";
  
  public String project = "";
  
  public String appConf = "";
  public int appId = 0;
  
  public NgixGateway gateway;
  public Cluster cluster;
  public int port ;
  public boolean isDistribution = true;
  
  
  public abstract String getName();
  
  public void setConf(Configuration conf )
  {
    conf.set("yarn.resourcemanager.address",this.cluster.resourceManagerAddress);
    conf.set("fs.defaultFS",this.cluster.defaultFS);
  }
}
