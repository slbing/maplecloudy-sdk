package com.maplecloudy.geoip.model;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.BinaryData;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.FixedSize;

@FixedSize(8)
public class LocalId implements GenericFixed, Comparable<LocalId> {
  public static Schema schema = ReflectData.get().getSchema(LocalId.class);
  
  public LocalId() {}
  
  // the 0-1 bytes is country, the 2-3 bytes is province, the 4-5 bytes is city, the 6-7 is isp
  byte[] bytes = new byte[8];
  
  public LocalId(int country, int province, int city, int isp) {
    int start = 0;
    short2byte((short) country, bytes, start);
    start = 2;
    short2byte((short) province, bytes, start);
    start = 4;
    short2byte((short) city, bytes, start);
    start = 6;
    short2byte((short) isp, bytes, start);
  }
  
  public LocalId(String local) {
    String[] arr = local.split("-");
    int country = 0;
    int province = 0;
    int city = 0;
    int isp = 0;
    if (arr.length > 0) country = Integer.parseInt(arr[0]);
    if (arr.length > 1) province = Integer.parseInt(arr[1]);
    if (arr.length > 2) city = Integer.parseInt(arr[2]);
    if (arr.length > 3 ) isp = Integer.parseInt(arr[3]);
    int start = 0;
    short2byte((short) country, bytes, start);
    start = 2;
    short2byte((short) province, bytes, start);
    start = 4;
    short2byte((short) city, bytes, start);
    start = 6;
    short2byte((short) isp, bytes, start);
    
  }
  
  public LocalId(String country, String province, String city, String isp) {
    this(Integer.parseInt(country), Integer.parseInt(province), Integer
        .parseInt(city), Integer.parseInt(isp));
    
  }
  
  public LocalId(byte[] bytes) {
    this.bytes = bytes;
  }
  
  public int getCountryCode() {
    return byte2short(bytes, 0);
  }
  
  public int getProvinceCode() {
    return byte2short(bytes, 2);
  }
  
  public int getCityCode() {
    return byte2short(bytes, 4);
  }
  public int getIspCode() {
	  return byte2short(bytes, 6);
  }
  
  public LocalId getParent() {
    if (this.getCityCode() != 0) return new LocalId(this.getCountryCode(),
        this.getProvinceCode(), 0,this.getIspCode());
    else if (this.getProvinceCode() != 0) return new LocalId(
        this.getCountryCode(), 0, 0, this.getIspCode());
    else return null;
    
  }
  
  @Override
  public Schema getSchema() {
    String space = LocalId.class.getPackage() == null ? "" : LocalId.class
        .getPackage().getName();
    return Schema.createFixed(LocalId.class.getSimpleName(), null /* doc */,
        space, 6);
  }
  
  @Override
  public byte[] bytes() {
    return bytes;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    return o instanceof GenericFixed
        && Arrays.equals(bytes, ((GenericFixed) o).bytes());
  }
  
  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }
  
  @Override
  public String toString() {
    // return Arrays.toString(bytes);
    return this.getCountryCode()+"-"+this.getProvinceCode()+"-"+this.getCityCode() + "-" +
    		this.getIspCode();
  }
  
  public int compareTo(LocalId that) {
    return BinaryData.compareBytes(this.bytes, 0, this.bytes.length,
        that.bytes, 0, that.bytes.length);
  }
  
  public void short2byte(short data, byte[] bytes, int start) {
    bytes[start] = (byte) ((data >> 8) & 0xff);
    bytes[start + 1] = (byte) (data & 0xff);
  }
  
  public static short byte2short(byte[] data, int start) {
    if (data == null || data.length < start + 1) {
      return 0;
    }
    return (short) ((data[start] & 0x00FF) << 8 | data[start + 1] & 0x00ff);
  }
}