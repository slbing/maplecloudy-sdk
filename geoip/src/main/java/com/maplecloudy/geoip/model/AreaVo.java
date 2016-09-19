package com.maplecloudy.geoip.model;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import com.maplecloudy.avro.reflect.ReflectDataEx;

/**
 * @author cc
 * @creation date 2012-7-20 下午05:34:26
 * @Description 地区vo 存地区编码，父编码，地区名称
 * @version 1.0
 */
public class AreaVo implements Comparable<AreaVo> {
  public static Schema schema = ReflectData.get().getSchema(AreaVo.class);
  // public String name;
  public String country;
  public String province;
  public String city;
  public String isp;
  
  public AreaVo() {}
  
  public AreaVo(String country, String province, String city, String isp) {
    this.country = country;
    this.province = province;
    this.city = city;
    this.isp = isp;
  }
  
  public String toString() {
    return country + " " + province + " " + city + " " + isp;
  }
  
  public String getCountry() {
    return this.country;
  }
  
  public String getProvince() {
    return this.province;
  }
  
  public String getCity() {
    return this.city;
  }
  
  public String getIsp() {
    return this.isp;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o == this) return true; // identical object
    if (!(o instanceof AreaVo)) return false; // not a record
    AreaVo that = (AreaVo) o;
    return ReflectDataEx.get().compare(this, that, schema) == 0;
  }
  
  @Override
  public int hashCode() {
    return ReflectDataEx.get().hashCode(this, schema);
  }
  
  @Override
  public int compareTo(AreaVo that) {
    return ReflectDataEx.get().compare(this, that, schema);
  }
}
