package com.maplecloudy.avro.util;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.junit.Test;

import com.maplecloudy.avro.util.AvroUtils;


public class AvroUtilsTest {
  public static class Mytest  {
    public String name = "zb";
    public int id = 98;
  }
  
  @Test
  public void testToJson() throws IOException
  {
    Mytest t = new Mytest();
    System.out.println(AvroUtils.toAvroString(t,false));
    System.out.println(AvroUtils.toAvroString(t));
  }
  
  @Test
  public void testReflect() throws SecurityException, NoSuchMethodException
  {
    System.out.println(ReflectDatumReader.class.getConstructor(Schema.class,Schema.class,ReflectData.class));
  }
}
