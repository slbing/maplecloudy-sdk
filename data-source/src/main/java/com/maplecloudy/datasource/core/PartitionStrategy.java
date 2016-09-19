package com.maplecloudy.datasource.core;

import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.maplecloudy.datasource.utils.DSUtil;
//strategy urlregex /$user/$dsname/$time/$hash
public class PartitionStrategy {
  
  public String strategyStr;
  public List<StrategyUnit> strategyLayer;
  
  public PartitionStrategy(String strategy) throws Exception {
    this.strategyStr = strategy;
    init();
  }
  
  protected void init() throws Exception {
    strategyLayer = Lists.newArrayList();
    JsonParser parser = DSUtil.JSON_FACTORY.createJsonParser(this.strategyStr);
    parser.setCodec(new ObjectMapper());
    Iterator<JsonNode> iter = parser.readValuesAs(JsonNode.class);
    for (JsonNode node : iter.next()) {
      StrategyUnit unit = new StrategyUnit();
      unit.sname = node.has("sname") ? node.get("sname").textValue() : null;
      unit.stype = node.has("stype") ? getStrategyFieldTypeClass(node.get(
          "stype").textValue()) : null;
      
      if (node.has("name")) unit.name = node.get("name").textValue();
      else throw new Exception("Parittion Strategy Format Exception: '"
          + node.toString() + "' must be include 'name'");
      if (node.has("type")) unit.type = getStrategyFieldTypeClass(node.get(
          "type").textValue());
      else throw new Exception("Parittion Strategy Format Exception: '"
          + node.toString() + "' must be include 'type'");
      strategyLayer.add(unit);
    }
    parser.close();
  }
  
  public StrategyUnit get(int degree) {
    if (!has(degree)) return null;
    
    return strategyLayer.get(degree);
  }
  
  public boolean has(int degree) {
    if (null == this.strategyLayer) return false;
    if (degree >= this.strategyLayer.size()) return false;
    else return true;
  }
  
  public String toString() {
    return "strategyStr: " + this.strategyStr + ", strategyLayer: "
        + this.strategyLayer;
  }
  
  protected Class<?> getStrategyFieldTypeClass(String type)
      throws ClassNotFoundException {
    type = type.toUpperCase();
    try {
      return StrategyFieldType.valueOf(type).typeClass();
    } catch (Exception e) {
      return Class.forName(type);
    }
  }
  
  public static enum StrategyFieldType {
    STRING(String.class), CHAR(Character.class), CHARACTER(Character.class), BOOLEAN(
        Boolean.class), INT(Integer.class), INTEGER(Integer.class), LONG(
        Long.class), FLOAT(Float.class), DOUBLE(Double.class), SHORT(
        Short.class);
    
    private Class<?> typeClass;
    
    StrategyFieldType(Class<?> typeClass) {
      this.typeClass = typeClass;
    }
    
    public Class<?> typeClass() {
      return typeClass;
    }
  }
  
  public static class StrategyUnit {
    public String name;
    public String sname;
    public Class<?> type;
    public Class<?> stype;
    
    public String toString() {
      return "[name: " + name + ", type: "
          + (null != type ? type.getSimpleName() : null) + ", sname: " + sname
          + ", stype: " + (null != stype ? stype.getSimpleName() : null) + "]";
    }
  }
  
  public static void main(String[] args) throws Exception {
    String s = "[{\"name\": \"bar\",\"type\": \"String\"},{\"name\": \"biz\", \"type\": \"long\"}]";
    
    PartitionStrategy strategy = new PartitionStrategy(s);
    
    System.out.println(strategy.toString());
  }
}
