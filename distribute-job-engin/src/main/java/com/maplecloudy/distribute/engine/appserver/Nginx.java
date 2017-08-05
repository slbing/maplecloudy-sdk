package com.maplecloudy.distribute.engine.appserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class Nginx {
  
  public static Map<String,Nginx> nginxs = Maps.newHashMap();
  public String name;
  
  public Map<String,NginxConf> apps = Maps.newHashMap();
  
  public void store() throws IOException {
    File nginxF = new File(name);
    for (Map.Entry<String,NginxConf> entry : apps.entrySet()) {
      if (!nginxF.exists()) nginxF.mkdirs();
      File nginxConfF = new File(nginxF, entry.getKey());
      Gson gson = new GsonBuilder().create();
      FileWriter wr = new FileWriter(nginxConfF);
      gson.toJson(entry.getValue(), wr);
      wr.flush();
      wr.close();
    }
  }
  
  public void load() throws JsonSyntaxException, JsonIOException,
      FileNotFoundException {
    apps.clear();
    Gson gson = new GsonBuilder().create();
    File nginxF = new File(name);
    if (nginxF.exists()) {
      File[] confs = nginxF.listFiles();
      for (File conf : confs) {
        NginxConf nc = gson.fromJson(new FileReader(conf), NginxConf.class);
        apps.put(conf.getName(), nc);
      }
    }
  }
  
  public void generateNginxConf() throws IOException {
    File nginxF = new File(name);
    for (Map.Entry<String,NginxConf> entry : apps.entrySet()) {
      if (!nginxF.exists()) nginxF.mkdirs();
      File nginxConfF = new File(nginxF, entry.getKey() + ".conf");
      
      FileWriter wr = new FileWriter(nginxConfF);
      for (Map.Entry<String,ProxyServer> pss : entry.getValue().psm.entrySet()) {
        wr.append(pss.getValue().getStoreContent());
      }
      wr.flush();
      wr.close();
    }
  }
  
  public Nginx getNgin(AppPara para) {
    if (nginxs.get(para.nginxIp) != null) {
      return nginxs.get(para.nginxIp);
    } else {
      WebProxyServer ps = new WebProxyServer();
      Nginx nginx = new Nginx();
      NginxConf nc = new NginxConf();
      nginx.apps.put(para.appConf, nc);
      nginxs.put(para.nginxIp, nginx);
      return nginx;
    }
  }
  
  public void update()
  {
    
  }
}
