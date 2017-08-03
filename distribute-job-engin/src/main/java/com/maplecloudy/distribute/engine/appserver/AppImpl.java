package com.maplecloudy.distribute.engine.appserver;

import java.util.List;

import com.maplecloudy.distribute.engine.app.kibana.StartKibanaTask;
import com.maplecloudy.distribute.engine.apptask.AppTask;
import com.maplecloudy.distribute.engine.apptask.TaskPool;

public class AppImpl implements IApp {
  
  public AppImpl() {
    
  }
  
  public int startKibana(AppPara para) {
    StartKibanaTask task = new StartKibanaTask(para);
    TaskPool.addTask(task);
    return 0;
  }
  
  @Override
  public List<String> getTaskInfo(AppPara para) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public AppStatus getAppStatus(AppPara para) {
    try {
      AppTask task = TaskPool.taskMap.get(para.getName());
      return task.getAppStatus();
    } catch (Exception e) {
      AppStatus as = new AppStatus();
      as.error = "get app info error with:" + e.getMessage();
      return as;
    }
  }
  
}
