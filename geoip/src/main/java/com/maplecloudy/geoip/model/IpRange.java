package com.maplecloudy.geoip.model;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import com.maplecloudy.avro.reflect.ReflectDataEx;

/**
 * @author cc
 * @description ip区间查找bean，包括ip的开始地址，结束地址，对应的地区id
 */
public class IpRange implements Comparable<IpRange> {
  public static Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"IpRange\",\"namespace\":\"com.maplecloudy.geoip.model\",\"fields\":[{\"name\":\"startNum\",\"type\":\"long\"},{\"name\":\"endNum\",\"type\":\"long\",\"order\":\"descending\"}]}");
  public long startNum;
  public long endNum;
  
  public IpRange() {
    
  }
  
  public IpRange(long startNum, long endNum) {
    this.startNum = startNum;
    this.endNum = endNum;
  }
  
  public String toString() {
    return iplongToIp(startNum)+"-"+iplongToIp(endNum);
  }
  
  public String iplongToIp(long ipaddress) {
    StringBuffer sb = new StringBuffer("");
    sb.append(String.valueOf((ipaddress >>> 24)));
    sb.append(".");
    sb.append(String.valueOf((ipaddress & 0x00FFFFFF) >>> 16));
    sb.append(".");
    sb.append(String.valueOf((ipaddress & 0x0000FFFF) >>> 8));
    sb.append(".");
    sb.append(String.valueOf((ipaddress & 0x000000FF)));
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o == this) return true; // identical object
    if (!(o instanceof IpRange)) return false; // not a record
    IpRange that = (IpRange) o;
    return ReflectData.get().compare(this, that, schema) == 0;
  }
  
  @Override
  public int hashCode() {
    return ReflectData.get().hashCode(this, schema);
  }
  
  @Override
  public int compareTo(IpRange that) {
    return ReflectDataEx.get().compare(this, that, schema);
  }
}
