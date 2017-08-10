package com.maplecloudy.distribute.engine.appserver;

import java.util.List;

import com.google.common.collect.Lists;
import com.maplecloudy.distribute.engine.app.jetty.JettyPara;
import com.maplecloudy.distribute.engine.app.jetty.StartJettyTask;
import com.maplecloudy.distribute.engine.app.elasticsearch.ElasticSearchTask;
import com.maplecloudy.distribute.engine.app.elasticsearch.ElatisticSearchPara;
import com.maplecloudy.distribute.engine.app.kibana.KibanaPara;
import com.maplecloudy.distribute.engine.app.kibana.StartKibanaTask;
import com.maplecloudy.distribute.engine.apptask.AppTask;
import com.maplecloudy.distribute.engine.apptask.TaskPool;

public class AppImpl implements IApp {
  
  public AppImpl() {
    
  }

  public int startElasticSearch(ElatisticSearchPara para)
  {
    ElasticSearchTask task = new ElasticSearchTask(para);
    TaskPool.addTask(task);
    return 0;
  } 
  public int startKibana(KibanaPara para) {
    System.out.println("startKibana:" + para.getName());
    StartKibanaTask task = new StartKibanaTask(para);
    TaskPool.addTask(task);
    return 0;
  }
  
  @Override
  public List<AppStatus> getAppStatus(KibanaPara para) {
    System.out.println("getAppStatus:" + para.getName());
    
    try {
      AppTask task = TaskPool.taskMap.get(para.getName());
      if (task == null) {
        new StartKibanaTask(para).checkTaskApp();
      }
      return task.getAppStatus();
    } catch (Exception e) {
      e.printStackTrace();
      AppStatus as = new AppStatus();
      as.error = "get app info error with:" + e.getMessage();
      return Lists.newArrayList();
    }
  }
  
  public List<String> getAppTaskInfo(KibanaPara para) {
    System.out.println("getAppTaskInfo:" + para.getName());
    AppTask task = TaskPool.taskMap.get(para.getName());
    if (task != null) {
      return task.runInfo;
    } else {
      return Lists.newArrayList();
    }
  }
  
  @Override
  public int stopAppTask(KibanaPara para) {
    System.out.println("stopAppTaskInfo:" + para.getName());
    try {
      AppTask task = TaskPool.taskMap.get(para.getName());
      if (task == null) {
        task = new StartKibanaTask(para);
        TaskPool.taskMap.put(para.getName(), task);
      }
      return task.stopApp();
    } catch (Exception e) {
      return -1;
    }
  }
  
  public int startJetty(JettyPara para) {
    System.out.println("startKibana:" + para.getName());
    StartJettyTask task = new StartJettyTask(para);
    TaskPool.addTask(task);
    return 0;
  }
  
  @Override
  public List<AppStatus> getJettyStatus(JettyPara para) {
    System.out.println("getAppStatus:" + para.getName());
    
    try {
      AppTask task = TaskPool.taskMap.get(para.getName());
      if (task == null) {
        new StartKibanaTask(para).checkTaskApp();
      }
      return task.getAppStatus();
    } catch (Exception e) {
      e.printStackTrace();
      AppStatus as = new AppStatus();
      as.error = "get app info error with:" + e.getMessage();
      return Lists.newArrayList();
    }
  }
  
  public List<String> getJettyTaskInfo(JettyPara para) {
    System.out.println("getAppTaskInfo:" + para.getName());
    AppTask task = TaskPool.taskMap.get(para.getName());
    if (task != null) {
      return task.runInfo;
    } else {
      return Lists.newArrayList();
    }
  }
  
  @Override
  public int stopJettyTask(JettyPara para) {
    System.out.println("stopAppTaskInfo:" + para.getName());
    try {
      AppTask task = TaskPool.taskMap.get(para.getName());
      if (task == null) {
        task = new StartKibanaTask(para);
        TaskPool.taskMap.put(para.getName(), task);
      }
      return task.stopApp();
    } catch (Exception e) {
      return -1;
    }
  }
  
  public static void main(String args[]) throws InterruptedException {
    
    String type;
    AppImpl server = new AppImpl();
    
    // type = "kibana";
    type = "jetty";
    
    if (type == "kibana") {
      KibanaPara para = new KibanaPara();
      
      para.user = "maplecloudy";
      para.project = "11";
      para.appConf = "11";
//      para.appId = 0;
      para.memory = 1024;
      para.cpu = 1;
      
      para.domain = "kibana01";
      para.nginxIp = "10.0.1.1";
      para.nginxDomain = "maplecloudy.com";
      para.port = 8888;
      
      para.defaultFS = "hdfs://hadoop02.aliyun.bj.maplecloudy.com:8020";
      para.resourceManagerAddress = "hadoop02.aliyun.bj.maplecloudy.com:8032";
      
      para.isDistribution = false;
      para.elasticsearchUrl = "http://10.0.16.21:9200";
      para.sc = "";
      
      server.startKibana(para);
    } else if (type == "jetty") {
      JettyPara para = new JettyPara();
      
      para.user = "gxiang";
      para.project = "11";
      para.appConf = "11";
//      para.appId = 0;
      para.memory = 1024;
      para.cpu = 1;
      
      para.domain = "jetty01";
      para.nginxIp = "60.205.171.123";
      para.nginxDomain = "maplecloudy.com";
      para.port = 8887;
      
      para.defaultFS = "hdfs://hadoop02.aliyun.bj.maplecloudy.com:8020";
      para.resourceManagerAddress = "hadoop02.aliyun.bj.maplecloudy.com:8032";
      
      para.isDistribution = false;
      para.elasticsearchUrl = "http://10.0.16.21:9200";
      para.sc = "";
      
      para.warFile = "maple/gxiang/rs-dist-0.0.1-SNAPSHOT-bin/lib/rs-0.0.1-SNAPSHOT.war";
      server.startJetty(para);
      Thread.sleep(1000);
      List<AppStatus> la = server.getJettyStatus(para);
      Thread.sleep(1000);
      List<String> ls = server.getJettyTaskInfo(para);    
      Thread.sleep(1000);
      server.stopJettyTask(para);
    }
  }
}
