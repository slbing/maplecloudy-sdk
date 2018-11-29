package com.maplecloudy.distribute.engine.appserver;

import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.Lists;
import com.maplecloudy.distribute.engine.app.engineapp.ClusterEngineAppTask;
import com.maplecloudy.distribute.engine.app.engineapp.EngineAppTask;
import com.maplecloudy.distribute.engine.apptask.AppTaskBaseline;
import com.maplecloudy.distribute.engine.apptask.TaskPool;

public class AppImpl implements IApp {
  
  public AppImpl() {
    
  }
  
  @Override
  public int startEngineApp(String para) throws JSONException {
    
   
    JSONObject json = new JSONObject(para);
    JSONArray jarr = json.getJSONArray("appId");
    System.out.println("start engine json received:" + para);
    for (int i = 0; i < jarr.length(); i++) {
      
      AppTaskBaseline task;
      JSONObject jtask = new JSONObject(para);
      jtask.remove("appId");
      jtask.put("appId", jarr.getInt(i));
      System.out.println("start engine :" + jtask);
      task = new EngineAppTask(jtask);
      TaskPool.addTask(task);
    }
    
    return 0;
  }
  
  @Override
  public List<AppStatus> getAppStatus(String para) {
    
    JSONObject json = null;
    String appName = "";
    try {
      json = new JSONObject(para);
      json.put("appId", json.getJSONArray("appId").getInt(0));
      appName = json.getString("user") + "|" + json.getString("project") + "|"
          + json.getString("appConf") + "|" + json.getString("appId");
      
    } catch (JSONException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    System.out.println("getAppStatus:" + appName);
    
    try {
      AppTaskBaseline task = TaskPool.taskMap.get(appName);
      if (task == null) {
        
          task = new EngineAppTask(json);
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
      json.put("appId", json.getJSONArray("appId").getInt(0));
      appName = json.getString("user") + "|" + json.getString("project") + "|"
          + json.getString("appConf") + "|" + json.getString("appId");
      
    } catch (JSONException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    System.out.println("getAppTaskInfo:" + appName);
    
    AppTaskBaseline task = TaskPool.taskMap.get(appName);
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
          AppTaskBaseline task = TaskPool.taskMap.get(appName);
          if (task == null) {
            task = new EngineAppTask(json);
            TaskPool.taskMap.put(appName, task);
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
    
    AppImpl server = new AppImpl();
   
    JSONObject json = new JSONObject();

    json.put("user", "gxiang");
    json.put("project", "119");
    JSONArray jarr = new JSONArray();
    jarr.put(268);
    json.put("appId", jarr);
    json.put("appConf", "37");
    json.put("memory", 1024);
    json.put("cpu", 1);
    json.put("domain", "www.ads");
    json.put("nginxIp", "60.205.171.123");
    json.put("nginxDomain", "maplecloudy.com");
    json.put("port", 0);
    json.put("proxyPort", 55552);
    json.put("defaultFS", "hdfs://hadoop02.aliyun.bj.maplecloudy.com:8020");
    json.put("resourceManagerAddress",
        "hadoop02.aliyun.bj.maplecloudy.com:8032");
    json.put("isDistribution", false);
    json.put("run.shell", "sh build.sh -fs demo.zip/home/maple/.maple/user/gxiang/build/gxiang/rs/rs-dist/target/rs-dist-0.0.1-SNAPSHOT-bin /user/gxiang/maple/gxiang gxiang");
    json.put("type", "BUILD");
    json.put("damon", true);
    json.put("nginx", false);
    
    //
    JSONArray confFiles = new JSONArray();
    
    JSONObject shell = new JSONObject();
    shell.put("fileName", "build.sh");
     
    
    confFiles.put(shell);

    //
    JSONArray files = new JSONArray();
    
    //
    JSONArray arcs = new JSONArray();
    arcs.put("/user/gxiang/demo.zip");
    arcs.put("/user/maplecloudy/apache-maven-3.3.9.zip");
    arcs.put("/user/maplecloudy/settings.xml");

    json.put("conf.files", confFiles);
    json.put("files", files);
    json.put("arcs", arcs);
    
    System.out.println(json);
   

//    String a = "{'user':'gxiang','project':'13','appId':['203'],'appConf':'13','memory':2560,'cpu':1,'containers':1,'ammemory':64,'amcpu':1,'priority':-1,'domain':'elasticsearch','nginxIp':'60.205.171.123','nginxDomain':'maplecloudy.com','port':0,'proxyPort':55552,'defaultFS':'hdfs://wx02.maplecloudy.com:8020','resourceManagerAddress':'wx01.maplecloudy.com:8032','isDistribution':false,'isCluster':true,'run.shell':'sh elasticsearch.sh','type':'ELASTICSEARCH','conf.files':[{'fileName':'elasticsearch.yml','<network.host>':'0.0.0.0','<path.data>':'/tmp/elasticsearch/mycluster/data','<path.logs>':'/tmp/elasticsearch/mycluster/logs','<cluster.name>':'mycluster'},{'fileName':'jvm.options','<Xms>':'-Xms2048m','<Xmx>':'-Xmx2048m'},{'fileName':'elasticsearch.sh'}],'files':[],'arcs':['/user/maplecloudy/com/elasticsearch/elasticsearch/5.3.0/elasticsearch-5.3.0.zip','/user/maplecloudy/com/java/jdk/8u121/jdk-8u121-linux-x64.tar.gz']}";
   String a = "{'isDistribution': False, 'domain': 'elasticsearch', 'conf.files': [{'<path.logs>': '/tmp/elasticsearch/es-11-2-66/logs', '<path.data>': '/tmp/elasticsearch/es-11-2-66/data', '<cluster.name>': 'es-11-2-66', '<network.host>': '0.0.0.0', 'fileName': 'elasticsearch.yml'}, {'<Xmx>': '-Xmx2048m', '<Xms>': '-Xms2048m', 'fileName': 'jvm.options'}, {'fileName': 'elasticsearch.sh'}], 'isCluster': True, 'port': 0, 'priority': -1, 'nginxIp': '10.20.4.103', 'appConf': '2', 'memory': 2560, 'type': 'ELASTICSEARCH', 'containers': 1, 'files': [], 'run.shell': 'sh elasticsearch.sh', 'arcs': ['/user/maplecloudy/com/elasticsearch/elasticsearch/5.3.0/elasticsearch-5.3.0.zip', '/user/maplecloudy/com/java/jdk/8u121/jdk-8u121-linux-x64.tar.gz'], 'user': 'zhongyi', 'appId': [66], 'ammemory': 64, 'proxyPort': 7080, 'resourceManagerAddress': 'wx01.maplecloudy.com:8032', 'project': '11', 'amcpu': 1, 'nginxDomain': 'wx05.maplecloudy.com', 'defaultFS': 'hdfs://wx02.maplecloudy.com:8020', 'cpu': 1}"; 
    server.startEngineApp(a);
//    Thread.sleep(10000);
//    List<AppStatus> la = server.getAppStatus(json.toString());
//    Thread.sleep(1000);
//    List<String> ls = server.getAppTaskInfo(json.toString());
    
//    Thread.sleep(20000);
//    server.stopAppTask(a);

  }
  
}
