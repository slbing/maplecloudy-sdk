package com.maplecloudy.distribute.engine.appserver;

import com.maplecloudy.distribute.engine.task.TaskAction;

public interface IApp {

  public int startKibana(AppPara para);
  public String startKibanaNum(String msg, int i);
  public Status checkStatus(String msg, int i);
  public String putStatus(Status stats);
  
  
  public Status processTask(TaskAction ta);
}
