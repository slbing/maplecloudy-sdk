package com.maplecloudy.distribute.engine.appserver;

import org.apache.avro.reflect.Nullable;

public class AppStatus {
    
  public String appid = "";
  public String appStatus = "";
  public String diagnostics = "";
  
  @Nullable
  public String error = null;
  public String host = "";
}
