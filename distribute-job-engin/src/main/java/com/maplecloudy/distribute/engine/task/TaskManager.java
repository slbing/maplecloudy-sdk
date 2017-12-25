package com.maplecloudy.distribute.engine.task;

import java.lang.reflect.InvocationTargetException;

public class TaskManager {
  
  public static void processTask(TaskAction ta)
  {
    
    ITask task = new KibanaTask();
    task.initialise(ta);
    try {
      task.submmit();
    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException
        | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
