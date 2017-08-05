package com.maplecloudy.distribute.engine.appserver;

import java.util.Map;

import com.google.common.collect.Maps;

public class NginxConf {
  public String name;//表示这个配置对应应用的类型
  public Map<String,ProxyServer> psm = Maps.newHashMap();
  
  
}
