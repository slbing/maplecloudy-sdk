package com.maplecloudy.distribute.engine.apptask;

import java.util.HashMap;

public class TaskPool {
//  private static ExecutorService cachedThreadPool = Executors
//      .newCachedThreadPool();
  
  public static HashMap<String,AppTaskBaseline> taskMap = new HashMap<String,AppTaskBaseline>();
  
  public static void addTask(AppTaskBaseline runable) {
    if (taskMap.get(runable.getName()) != null)
      taskMap.remove(runable.getName());
    taskMap.put(runable.getName(), runable);
    Thread task = new Thread(runable,runable.getName());
    task.start();
//    cachedThreadPool.execute(runable);
  }
}
