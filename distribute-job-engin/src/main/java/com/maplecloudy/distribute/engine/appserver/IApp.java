package com.maplecloudy.distribute.engine.appserver;

import java.util.List;

import org.codehaus.jettison.json.JSONException;


public interface IApp {

  int startEngineApp(String para) throws JSONException;

  List<AppStatus> getAppStatus(String para);

  int stopAppTask(String para);

  List<String> getAppTaskInfo(String para);

 
}
