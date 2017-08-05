package com.maplecloudy.distribute.engine.appserver;

import java.util.List;

import com.google.common.collect.Lists;
import com.maplecloudy.distribute.engine.app.kibana.KibanaPara;
import com.maplecloudy.distribute.engine.app.kibana.StartKibanaTask;
import com.maplecloudy.distribute.engine.apptask.AppTask;
import com.maplecloudy.distribute.engine.apptask.TaskPool;

public class AppImpl implements IApp {
  
  public AppImpl() {
    
  }
  
  public int startKibana(KibanaPara para) {
    StartKibanaTask task = new StartKibanaTask(para);
    TaskPool.addTask(task);
    return 0;
  }
  
  @Override
  public List<AppStatus> getAppStatus(KibanaPara para) {
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
    AppTask task = TaskPool.taskMap.get(para.getName());
    if (task != null) {
      return task.checkInfo;
    } else {
      return Lists.newArrayList();
    }
  }
  
  @Override
  public int stopAppTask(KibanaPara para) {
    try {
      AppTask task = TaskPool.taskMap.get(para.getName());
      if (task == null) {
        new StartKibanaTask(para);
      }
      return task.stopApp();
    } catch (Exception e) {
      return -1;
    }
  }
  
}
