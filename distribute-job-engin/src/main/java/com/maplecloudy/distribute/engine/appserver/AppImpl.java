package com.maplecloudy.distribute.engine.appserver;

import com.maplecloudy.distribute.engine.app.kibana.StartKibanaTask;
import com.maplecloudy.distribute.engine.apptask.TaskPool;
import com.maplecloudy.distribute.engine.task.TaskAction;

public class AppImpl implements IApp {
  
  public AppImpl() {
    
  }
  
  public int startKibana(AppPara para) {
    StartKibanaTask task = new StartKibanaTask(para);
    TaskPool.addTask(task);
    return 0;
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
  public String startKibanaNum(String msg, int i) {
    // TODO Auto-generated method stub
    return null;
  }
  
}
