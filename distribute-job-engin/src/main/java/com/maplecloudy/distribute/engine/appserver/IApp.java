package com.maplecloudy.distribute.engine.appserver;

import java.util.List;

import com.maplecloudy.distribute.engine.app.kibana.KibanaPara;

public interface IApp {

  public int startKibana(KibanaPara para);
//  public List<String> getTaskInfo(AppPara para);
  public AppStatus getAppStatus(KibanaPara para);
  
  public List<String> getAppTaskInfo(KibanaPara para);
 
}
