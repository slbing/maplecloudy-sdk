package com.maplecloudy.distribute.engine.appserver;

import org.apache.avro.reflect.Nullable;

public class AppStatus {
    
  public String appid = "";
  public String appStatus = "";
  public String diagnostics = "";
  public String trackUrl = "";
  
  @Nullable
  public String error = null;
  public String host = "";
  public int port = 0;
  @Override
  public String toString() {
    return "AppStatus [appid=" + appid + ", appStatus=" + appStatus
        + ", diagnostics=" + diagnostics + ", error=" + error + ", host=" + host
        + ", port=" + port + "]";
  }
  
}
