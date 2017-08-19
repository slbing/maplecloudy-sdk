package com.maplecloudy.distribute.engine.nginx;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

public class NginxServer {
  
  public String serverName;
  public Map<String,NginxAppType> nginxApps = Maps.newHashMap();
  
  public void generateNginxConf(String path) throws IOException {
    
    path = path + "/" + serverName;
    File serverNamePath = new File(path);
    
    for (Map.Entry<String,NginxAppType> nginxApp : nginxApps.entrySet()) {
      
      if (!serverNamePath.exists()) serverNamePath.mkdirs();
      nginxApp.getValue().generateNginxConf(path);
    }
    
  }
  
  public void updateLocal(NginxGatewayPara para) {
    // TODO Auto-generated method stub
    if (this.nginxApps.get(para.appType) == null) {
      
      NginxAppType nginxApp = new NginxAppType();
      nginxApp.appTypeName = para.appType;
      nginxApps.put(para.appType, nginxApp);
    }
    
    nginxApps.get(para.appType).updateLocal(para);
    
    
  
  }
  
}
