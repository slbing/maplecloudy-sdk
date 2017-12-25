package com.maplecloudy.distribute.engine.nginx;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

public class NginxAppConf {
  
  public String appConfName;
  public List<WebProxyServer> servers = new ArrayList();
  
  public final static String normalTmeplate = "#-----appname:<name>\n"
      + "server {\n" + " listen <proxyPort>;\n" + " access_log <logFile>;\n"
      + "\n" + " location / {\n"
      + " proxy_pass \"http://<appHost>:<appPort>\";\n"
      + " proxy_set_header Host $host:<appPort>;\n"
      + " proxy_set_header X-Real-IP $remote_addr;\n"
      + " proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n"
      + " proxy_set_header Via \"nginx\";\n" + " }\n" + "}\n\n";
  
  public final static String lbTmeplate = "#-----appname:<name>\n"
      
      + "upstream <name> {\n"
      + " #<server> \n"
      + " }\n"
 
      + "server {\n" + " listen <proxyPort>;\n" + " access_log <logFile>;\n"
      + "\n" + " location / {\n"
      + " proxy_pass \"http://<name>\";\n"
      + " proxy_set_header Host $host;\n"
      + " proxy_set_header X-Real-IP $remote_addr;\n"
      + " proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n"
      + " proxy_set_header Via \"nginx\";\n" + " }\n" + "}\n\n";
  
  public String getContent() {
    
    String ret = "";
    if (servers == null || servers.size() < 1) {
      
    } else if (servers.size() == 1) {
      
      ret = normalTmeplate;
      for (WebProxyServer server : servers) {
        ret = ret.replaceAll("<name>", appConfName)
            .replaceAll("<proxyPort>", "" + server.proxyPort)
            .replaceAll("<appPort>", "" + server.appPort)
            .replaceAll("<appHost>", "" + server.appHost)
            .replaceAll("<logFile>", server.name + ".log");
      }
    } else {
      
      ret = lbTmeplate;
      for (WebProxyServer server : servers) {
       
        ret = ret.replaceAll("<name>", appConfName)
            .replaceAll("<proxyPort>", "" + server.proxyPort)
            .replaceAll("<appPort>", "" + server.appPort)
            .replaceAll("<appHost>", "" + server.appHost)
            .replaceAll("<logFile>", server.name + ".log")
            .replaceAll("#<server>"," server "+server.appHost+":"+server.appPort+";\n #<server>");
      }
    }
    return ret;
  }
  
  public void updateLocal(NginxGatewayPara para) {
    
    if (!existServer(para)) {
      
      WebProxyServer wps = new WebProxyServer();
      wps.name = para.appConf;
      wps.appHost = para.appHost;
      wps.domain = para.domain;
      wps.appPort = para.appPort;
      wps.proxyPort = para.proxyPort;
      this.servers.add(wps);
    }
  }
  
  public boolean hasServer(NginxAppType nc, NginxGatewayPara para) {
    
    for (WebProxyServer wps : servers) {
      
      if (wps.appPort == para.appPort && wps.proxyPort == para.proxyPort
          && wps.appHost == para.appHost && wps.domain == para.domain)
        return true;
    }
    return false;
  }
  
  private boolean existServer(NginxGatewayPara para) {
    for (WebProxyServer wps : servers) {
      
      if (wps.appPort == para.appPort && wps.proxyPort == para.proxyPort
          && wps.appHost == para.appHost && wps.domain == para.domain)
        return true;
    }
    return false;
  }
}
