package com.maplecloudy.bi.model;

import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.BinaryData;
import org.apache.avro.specific.FixedSize;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.StringUtils;

import com.maplecloudy.avro.util.AvroUtils;

@FixedSize(16)
public class MD5Signature implements GenericFixed, Comparable<MD5Signature> {
  
  public MD5Signature() {}
  
  byte[] bytes = new byte[16];
  
  public MD5Signature(Object obj) throws IOException {
    bytes = calculate(obj);
//    byte[] barr = calculate(obj);
//    for (int i = 0; i < barr.length; i++) {
//      bytes[i] = barr[i];
//    }
  }
  
  public byte[] calculate(Object obj) throws IOException {
    DataOutputBuffer out =  AvroUtils.serialize(obj);
    out.write(obj.getClass().getName().getBytes());
//    if(obj instanceof PlatformConversionsKey)
//    System.out.println(obj+":"+StringUtils.byteToHexString(out.getData(),0,out.getLength()));
    return MD5Hash.digest(out.getData(),0,out.getLength()
        ).getDigest();
  }
  
  @Override
  public Schema getSchema() {
    String space = MD5Signature.class.getPackage() == null ? ""
        : MD5Signature.class.getPackage().getName();
    return Schema.createFixed(MD5Signature.class.getSimpleName(),
        null /* doc */, space, 16);
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
//    return Arrays.toString(bytes);
    return StringUtils.byteToHexString(bytes);
  }
  
  public int compareTo(MD5Signature that) {
    return BinaryData.compareBytes(this.bytes, 0, this.bytes.length,
        that.bytes, 0, that.bytes.length);
  }
}