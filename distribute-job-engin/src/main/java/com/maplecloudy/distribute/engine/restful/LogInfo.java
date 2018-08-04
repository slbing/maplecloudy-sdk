package com.maplecloudy.distribute.engine.restful;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.collect.Lists;

@XmlRootElement(name = "log")
@XmlAccessorType(XmlAccessType.FIELD)
public class LogInfo {
  public List<String> error = Lists.newArrayList();
  
  public String stdout = "";
  public String stderr = "";
  
  public LogInfo() {
    error.add("error is null");
  }
}
