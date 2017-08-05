package com.maplecloudy.distribute.engine.appserver;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class NginxGateway {
  
  public String id = ""; // userid+projectid+confid+
  public String confPath = "";
  public String portNginx; // nginx listen port
  public String hostAddress; // application host
  public String portApp; // app real port at its host
  public List<String> checkInfo = new ArrayList<String>();
  
  public static ArrayList<NginxGateway> arrNg = new ArrayList<NginxGateway>();
  public static ArrayList<NginxGateway> arrDel = new ArrayList<NginxGateway>();
  public static ArrayList<NginxGateway> arrAdd = new ArrayList<NginxGateway>();
  
  public static void addNginx(NginxGateway ng) {
    
    NginxGateway.arrAdd.add(ng);
    ng.checkInfo
        .add("nginx conf added to processing queue, waiting for update");
  }
  
  public static void removeNginx(NginxGateway ng) {
    
    NginxGateway.arrDel.add(ng);
    ng.checkInfo.add("app closed, nginx conf to be removed");
  }
  
  public static void mergeNginx() {
    
    for(int i = 0; i < NginxGateway.arrAdd.size();i++)
    {
      NginxGateway.arrNg.add(NginxGateway.arrAdd.get(i));
      NginxGateway.arrNg.get(i).checkInfo.add("passing to remote");
    }
    NginxGateway.arrAdd.clear();;

    for(int i = 0; i < NginxGateway.arrDel.size();i++)
    {
      NginxGateway.arrNg.add(NginxGateway.arrAdd.get(i));
      NginxGateway.arrNg.get(i).checkInfo.add("passing to remote");
    }
    NginxGateway.arrDel.clear();;
  }
  
  public static String getAppInfo(String appId) {
    return "";
  }
  
  public static String getConfigInfo(String confId) {
    return "";
  }
  
  public static void main(String args[]) {
    NginxGateway ng = new NginxGateway();
    ng.id = "1";
    ng.confPath = "";
    ng.portNginx = "5555";
    ng.hostAddress = "8080";
    ng.portApp = "";
    
    for(NginxGateway n:NginxGateway.arrAdd)
    {
      ng.checkInfo.add("passing to remote");
      NginxGateway.arrNg.add(n);
      NginxGateway.arrAdd.remove(n);
    }
  }
}