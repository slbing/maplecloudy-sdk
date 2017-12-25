package com.maplecloudy.distribute.engine.nginx;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

public class NginxAppType {
  
  public String appTypeName;// 表示这个配置对应应用的类型
  public Map<String,NginxAppConf> nginxAppConfs = Maps.newHashMap();
  
  public void generateNginxConf(String path) throws IOException {
    
    File nginxConf = new File(path, appTypeName + ".conf");
    FileWriter wr = new FileWriter(nginxConf);
    for (Map.Entry<String,NginxAppConf> nginxAppConf : nginxAppConfs
        .entrySet()) {
      
      wr.append(nginxAppConf.getValue().getContent());
    }
    wr.flush();
    wr.close();
  }
  
  public void updateLocal(NginxGatewayPara para) {
    // TODO Auto-generated method stub
    
    if (this.nginxAppConfs.get(para.appConf) == null) {
      
      NginxAppConf nginxAppConf = new NginxAppConf();
      nginxAppConf.appConfName = para.appConf;
      nginxAppConfs.put(para.appConf, nginxAppConf);
    }
    
    nginxAppConfs.get(para.appConf).updateLocal(para);
    
  }
  
}
