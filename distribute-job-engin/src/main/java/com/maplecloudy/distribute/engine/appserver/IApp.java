package com.maplecloudy.distribute.engine.appserver;

import java.util.List;

import com.maplecloudy.distribute.engine.app.jetty.JettyPara;
import com.maplecloudy.distribute.engine.app.kibana.KibanaPara;

public interface IApp {

  public int startKibana(KibanaPara para);
//  public List<String> getTaskInfo(AppPara para);
  public List<AppStatus> getAppStatus(KibanaPara para);
  
  public List<String> getAppTaskInfo(KibanaPara para);
  
  public int stopAppTask(KibanaPara para);
  

  public int startJetty(JettyPara para);
  
  public List<String> getJettyTaskInfo(JettyPara para);
  
  public List<AppStatus> getJettyStatus(JettyPara para);
  
  public int stopJettyTask(JettyPara para);
 
 
}
