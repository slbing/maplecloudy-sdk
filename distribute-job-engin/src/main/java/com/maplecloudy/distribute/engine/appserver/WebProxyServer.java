package com.maplecloudy.distribute.engine.appserver;

public class WebProxyServer extends ProxyServer {
  
  public String appHost;
  public int appPort;
  public String domain;
  public int proxyPort;
  
  public String getStoreContent() {
    String ret = ProxyServer.tmeplate;
    ret = ret.replaceAll("<name>", name)
        .replaceAll("<proxyPort>", "" + proxyPort)
        .replaceAll("<appPort>", "" + appPort)
        .replaceAll("<appHost>", "" + appHost).replaceAll("<logFile>", name+".log");
    return ret;
  }
  
}
