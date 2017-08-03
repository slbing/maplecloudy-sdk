package com.maplecloudy.distribute.engine.appserver;

import java.util.List;

public interface IApp {

  public int startKibana(AppPara para);
  public List<String> getTaskInfo(AppPara para);
  public AppStatus getAppStatus(AppPara para);
 
}
