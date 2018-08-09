package com.maplecloudy.distribute.engine.apptask;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskPool {
  private static ExecutorService cachedThreadPool = Executors
      .newCachedThreadPool();
  
  public static HashMap<String,AppTaskBaseline> taskMap = new HashMap<String,AppTaskBaseline>();
  
  public static void addTask(AppTaskBaseline runable) {
    if (taskMap.get(runable.getName()) != null)
      taskMap.remove(runable.getName());
    taskMap.put(runable.getName(), runable);
    cachedThreadPool.execute(runable);
  }
}
