package com.maplecloudy.distribute.engine.appserver;

import org.codehaus.jettison.json.JSONException;

import com.maplecloudy.distribute.engine.app.kibana.KibanaPara;

public interface IApp2 {

  int startEngineApp(String para) throws JSONException;

 
}
