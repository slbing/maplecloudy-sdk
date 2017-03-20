package com.maplecloudy.datasource.core;

import java.util.Map;

import org.apache.avro.Schema;

public class Meta {
  //meta的版本号，与此数据生成的时间戳为准，用于更新meta
  public long version;
  
  public long totalNum;
  Map<String,Map<Schema.Type,Long>> name2info;
  
  public void merge(Meta meta) {
    if (meta == null) return;
    this.totalNum += meta.totalNum;
    //to be coding...
  }
}
