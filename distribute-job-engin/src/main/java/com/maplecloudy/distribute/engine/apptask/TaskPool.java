package com.maplecloudy.distribute.engine.apptask;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskPool {
  private static  ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
  
  public static HashMap<String,Runnable> taskMap = new HashMap<String,Runnable>();
  
  public static void addTask(AppTask runable)
  {
    if(taskMap.get(runable.getName()) != null)
    {
      System.out.println("该task已经被创建，请查询状态！");
    }
    taskMap.put(runable.getName(), runable);
    cachedThreadPool.execute(runable);
  }
}
