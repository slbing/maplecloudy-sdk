package com.maplecloudy.distribute.engine.task;

import java.lang.reflect.InvocationTargetException;

public interface ITask {
  
  public void initialise(TaskAction ta);
  public void submmit() throws ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException;
}
