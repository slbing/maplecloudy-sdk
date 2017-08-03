package com.maplecloudy.distribute.engine.server;

import com.maplecloudy.distribute.engine.task.TaskAction;
import com.maplecloudy.distribute.engine.task.TaskManager;

public class AppImpl implements IApp {
  
  public AppImpl() {
    
  }
  
  public String startKibanaNum(String msg, int i) {
    // TODO Auto-generated method stub
    
    System.out.println("Start Kibana:" + msg + " i:" + i);
    return "100";
  }
  
  public Status checkStatus(String msg, int i) {
    // TODO Auto-generated method stub
    return new Status();
  }
  
  public String putStatus(Status stats) {
    // TODO Auto-generated method stub
    
    System.out.println("put status success:" + stats.name);
    return "put status success";
  }
  
  public Status processTask(TaskAction ta) {
    
//    TaskManager.processTask(ta);
    
    return new Status();
  }
  
  @Override
  public int startKibana(String msg) {
    // TODO Auto-generated method stub
    return 0;
  }
  
}
