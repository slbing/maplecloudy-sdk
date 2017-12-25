package com.maplecloudy.distribute.engine.task;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import com.maplecloudy.distribute.engine.MapleCloudyEngineClient;

public class KibanaTask implements ITask {
  
  String cmd[];
  
  public void submmit() throws ClassNotFoundException, NoSuchMethodException,
      SecurityException, IllegalAccessException, IllegalArgumentException,
      InvocationTargetException {
    
    cmd = new String[] {
        "-arc", "/user/maplecloudy/kibana.yml", "-sc",
        "/user/maplecloudy/kibana.sh", "-user", "gxiang", "-arc",
        "/user/maplecloudy/elasticsearch/kibana-5.3.0-linux-x86_64.zip",
        "-args", "sh kibana.sh", "-damon"};
    
    Class clazz = Class
        .forName("com.maplecloudy.distribute.engine.MapleCloudyEngineShellClient");
    
    Method mainMethod = clazz.getMethod("main", String[].class);
    
    mainMethod.invoke(null, new Object[] {cmd});
    
  }
  
  public void initialise(TaskAction ta) {
    // TODO Auto-generated method stub
    System.out.println(ta.typeName);
    System.out.println(ta.cmd);
  }
  
  public static void main(String args[]) {
    KibanaTask kt = new KibanaTask();
    try {
      kt.submmit();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
