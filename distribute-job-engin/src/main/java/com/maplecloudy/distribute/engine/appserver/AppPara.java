package com.maplecloudy.distribute.engine.appserver;

import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.conf.Configuration;

public abstract class AppPara {
  
  public String user = "maplecloudy";
  
  public String project = "";
  
  public String appConf = "";
  public int appId = 0;
  
//  @Nullable
//  public NgixGateway gateway;
//  public Cluster cluster;
  public String defaultFS ="";
  public String resourceManagerAddress = "";

  public int port ;
  public boolean isDistribution = true;
  
  
  public abstract String getName();
  
  public void setConf(Configuration conf )
  {
    conf.set("yarn.resourcemanager.address",this.resourceManagerAddress);
    conf.set("fs.defaultFS",this.defaultFS);
  }
}
