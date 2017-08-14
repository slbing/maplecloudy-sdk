package com.maplecloudy.distribute.engine.apptask;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.maplecloudy.distribute.engine.app.engineapp.EngineAppTask;

public class TaskPool2 {
 private static  ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
  
  public static HashMap<String,EngineAppTask> taskMap = new HashMap<String,EngineAppTask>();
  
  public static void addTask(EngineAppTask runable)
  {
    if(taskMap.get(runable.getName()) != null)
    {
      System.out.println("该task已经被创建，请查询状态！");
    }
    taskMap.put(runable.getName(), runable);
    cachedThreadPool.execute(runable);
  }
}
