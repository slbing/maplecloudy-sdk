package com.maplecloudy.distribute.engine.appserver;

import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.Lists;
import com.maplecloudy.distribute.engine.app.engineapp.EngineAppTask;
import com.maplecloudy.distribute.engine.app.kibana.KibanaInstallInfo;
import com.maplecloudy.distribute.engine.app.kibana.KibanaPara;
import com.maplecloudy.distribute.engine.app.kibana.StartKibanaTask;
import com.maplecloudy.distribute.engine.apptask.AppTask;
import com.maplecloudy.distribute.engine.apptask.AppTaskBaseline;
import com.maplecloudy.distribute.engine.apptask.TaskPool;
import com.maplecloudy.distribute.engine.apptask.TaskPool2;

public class AppImpl2 implements IApp2 {
  
  public AppImpl2() {
    
  }
  
  @Override
  public int startEngineApp(String para) throws JSONException {
    
    EngineAppTask task;
    JSONObject json = new JSONObject(para);
    JSONArray jarr = json.getJSONArray("appId");
    for (int i = 0; i < jarr.length(); i++) {
      
      json.put("appId", jarr.getInt(i));
      task = new EngineAppTask(json);
      TaskPool2.addTask(task);
    }
    
    return 0;
  }
  
  @Override
  public List<AppStatus> getAppStatus(String para) {
    
    JSONObject json = null;
    String appName = "";
    try {
      json = new JSONObject(para);
      appName = json.getString("user") + "|" + json.getString("project") + "|"
          + json.getString("appConf") + "|" + json.getString("appId");
      
    } catch (JSONException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    System.out.println("getAppStatus:" + appName);
    
    try {
      AppTaskBaseline task = TaskPool2.taskMap.get(appName);
      if (task == null) {
        new EngineAppTask(json).checkTaskApp();
      }
      return task.getAppStatus();
    } catch (Exception e) {
      e.printStackTrace();
      AppStatus as = new AppStatus();
      as.error = "get app info error with:" + e.getMessage();
      return Lists.newArrayList();
    }
  }
  
  @Override
  public List<String> getAppTaskInfo(String para) {
    JSONObject json = null;
    String appName = "";
    try {
      json = new JSONObject(para);
      appName = json.getString("user") + "|" + json.getString("project") + "|"
          + json.getString("appConf") + "|" + json.getString("appId");
      
    } catch (JSONException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    System.out.println("getAppTaskInfo:" + appName);
    
    EngineAppTask task = TaskPool2.taskMap.get(appName);
    if (task != null) {
      return task.runInfo;
    } else {
      return Lists.newArrayList();
    }
  }
  
  @Override
  public int stopAppTask(String para) {
    
    int ret = 0;
    JSONObject json = null;
    String appName = "";
    try {
      json = new JSONObject(para);
      
      JSONArray jarr = json.getJSONArray("appId");
      for (int i = 0; i < jarr.length(); i++) {
         
        appName = json.getString("user") + "|" + json.getString("project") + "|"
            + json.getString("appConf") + "|"
            + jarr.getInt(i);
        json.put("appId", jarr.getInt(i));
        
        System.out.println("stopAppTask:" + appName);
        
        try {
          EngineAppTask task = TaskPool2.taskMap.get(appName);
          if (task == null) {
            task = new EngineAppTask(json);
            TaskPool2.taskMap.put(appName, task);
          }
          if (ret == 0) ret = task.stopApp();
        } catch (Exception e) {
          return -1;
        }
      }
    } catch (JSONException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    return ret;
  }
  
  public static void main(String args[])
      throws InterruptedException, JSONException {
    
    AppImpl2 server = new AppImpl2();
    
    JSONObject json = new JSONObject();
    
    json.put("user", "gxiang");
    json.put("project", "13");
    JSONArray appArr = new JSONArray();
    appArr.put(203);
    appArr.put(204);
    appArr.put(205);
    json.put("appId", appArr);
    json.put("appConf", "13");
    json.put("memory", 1024);
    json.put("cpu", 1);
    json.put("domain", "kibana01");
    json.put("nginxIp", "60.205.171.123");
    json.put("nginxDomain", "maplecloudy.com");
    json.put("port", 0);
    json.put("proxyPort", 55552);
    json.put("defaultFS", "hdfs://hadoop02.aliyun.bj.maplecloudy.com:8020");
    json.put("resourceManagerAddress",
        "hadoop02.aliyun.bj.maplecloudy.com:8032");
    json.put("isDistribution", false);
    json.put("run.shell", "sh kibana.sh");
    json.put("type", "KIBANA");
    
    //
    JSONArray confFiles = new JSONArray();
    JSONObject file = new JSONObject();
    file.put("fileName", "kibana.yml");
    file.put("<port>", 5601);
    file.put("<es_url:es_port>", "10.0.16.21:9200");
    
    JSONObject shell = new JSONObject();
    shell.put("fileName", "kibana.sh");
    
    confFiles.put(file);
    confFiles.put(shell);
    
    //
    JSONArray files = new JSONArray();
    
    //
    JSONArray arcs = new JSONArray();
    arcs.put(
        "/user/maplecloudy/com/elasticsearch/kibana/5.3.0/kibana-5.3.0-linux-x86_64.zip");
    
    json.put("conf.files", confFiles);
    json.put("files", files);
    json.put("arcs", arcs);
    
    System.out.println(json);
    
    server.startEngineApp(json.toString());
    Thread.sleep(10000);
//    List<AppStatus> la = server.getAppStatus(json.toString());
//    Thread.sleep(1000);
//    List<String> ls = server.getAppTaskInfo(json.toString());
//    Thread.sleep(1000);
    server.stopAppTask(json.toString());
    
  }
  
}
