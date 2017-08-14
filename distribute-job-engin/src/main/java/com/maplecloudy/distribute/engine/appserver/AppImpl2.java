package com.maplecloudy.distribute.engine.appserver;

import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.Lists;
import com.maplecloudy.distribute.engine.app.engineapp.EngineAppTask;
import com.maplecloudy.distribute.engine.app.kibana.KibanaPara;
import com.maplecloudy.distribute.engine.app.kibana.StartKibanaTask;
import com.maplecloudy.distribute.engine.apptask.AppTask;
import com.maplecloudy.distribute.engine.apptask.TaskPool;
import com.maplecloudy.distribute.engine.apptask.TaskPool2;

public class AppImpl2 implements IApp2 {
  
  public AppImpl2() {
    
  }
  
  @Override
  public int startEngineApp(String para) throws JSONException {
    
    EngineAppTask task;
    JSONObject json = new JSONObject(para);
    
    task = new EngineAppTask(json);
    
    TaskPool2.addTask(task);
    
    return 0;
  }
  
  // @Override
  // public List<AppStatus> getAppStatus(KibanaPara para) {
  // System.out.println("getAppStatus:" + para.getName());
  //
  // try {
  // AppTask task = TaskPool.taskMap.get(para.getName());
  // if (task == null) {
  // new StartKibanaTask(para).checkTaskApp();
  // }
  // return task.getAppStatus();
  // } catch (Exception e) {
  // e.printStackTrace();
  // AppStatus as = new AppStatus();
  // as.error = "get app info error with:" + e.getMessage();
  // return Lists.newArrayList();
  // }
  // }
  //
  // public List<String> getAppTaskInfo(KibanaPara para) {
  // System.out.println("getAppTaskInfo:" + para.getName());
  // AppTask task = TaskPool.taskMap.get(para.getName());
  // if (task != null) {
  // return task.runInfo;
  // } else {
  // return Lists.newArrayList();
  // }
  // }
  //
  // @Override
  // public int stopAppTask(KibanaPara para) {
  // System.out.println("stopAppTaskInfo:" + para.getName());
  // try {
  // AppTask task = TaskPool.taskMap.get(para.getName());
  // if (task == null) {
  // task = new StartKibanaTask(para);
  // TaskPool.taskMap.put(para.getName(), task);
  // }
  // return task.stopApp();
  // } catch (Exception e) {
  // return -1;
  // }
  // }
  public static void main(String args[])
      throws InterruptedException, JSONException {
    
    String type = "";
    AppImpl2 server = new AppImpl2();
    
    JSONObject json = new JSONObject();
//    json.put("elasticsearchUrl", "http://10.0.16.21:9200");
    
//    json.put("sc", "");
    json.put("user", "gxiang");
    json.put("project", "12");
    json.put("appId", "202");
    json.put("appConf", "12");
    json.put("memory", 1024);
    json.put("cpu", 1);
    json.put("domain", "jetty01");
    json.put("nginxIp", "60.205.171.123");
    json.put("nginxDomain", "maplecloudy.com");
    json.put("port", 0);
    json.put("proxyPort", 55551);
    json.put("defaultFS", "hdfs://hadoop02.aliyun.bj.maplecloudy.com:8020");
    json.put("resourceManagerAddress",
        "hadoop02.aliyun.bj.maplecloudy.com:8032");
    json.put("isDistribution", false);
    json.put("run.shell", "sh jetty.sh");
    json.put("type", "JETTY");
    
   
    //
    JSONArray confFiles = new JSONArray();
    JSONObject file = new JSONObject();
    file.put("fileName", "jetty.xml");
    file.put("data.path", "/log");
    file.put("<port>", 8080);
    
    JSONObject shell = new JSONObject();
    shell.put("fileName", "jetty.sh");
    
    JSONObject war = new JSONObject();
    
    confFiles.put(file);
    confFiles.put(shell);
    
    //
    JSONArray files = new JSONArray();
    files.put("/user/gxiang/maple/gxiang/rs-dist-0.0.1-SNAPSHOT-bin/lib/rs-0.0.1-SNAPSHOT.war");
    
    //
    JSONArray arcs = new JSONArray();
    arcs.put("/user/maplecloudy/com/webserver/jetty/7.6.21/jetty-distribution-7.6.21.v20160908.zip");
    

    json.put("conf.files", confFiles);
    json.put("files", files);
    json.put("arcs", arcs);
    
    System.out.println(json);
    
    server.startEngineApp(json.toString());
    Thread.sleep(1000);
    // List<AppStatus> la = server.getJettyStatus(para);
    // Thread.sleep(1000);
    // List<String> ls = server.getJettyTaskInfo(para);
    // Thread.sleep(1000);
    // server.stopJettyTask(para);
    
  }
  
}
