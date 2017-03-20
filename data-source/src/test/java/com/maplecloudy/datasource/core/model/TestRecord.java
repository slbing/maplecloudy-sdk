package com.maplecloudy.datasource.core.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.junit.Test;


public class TestRecord {
  @Test
  public void testRecord() {
    System.currentTimeMillis();
//    Record rd = new Record();
//    rd.datum.put("1", new Any(1));
//    
//    List<Any> lany = new ArrayList<Any>();
//    lany.add(new Any(2));
//    lany.add(new Any("3"));
//    rd.datum.put("2", new Any(new LAny(lany)));
//    
//    Map<String,Any> many = new HashMap<String,Any>();
//    many.put("5", new Any(5));
//    many.put("6",new Any("6"));
//    System.out.println(toAvroString(rd,true));
    
  }
  static DataOutputBuffer out = new DataOutputBuffer();
  static DataInputBuffer in = new DataInputBuffer();
  public static <V> String toAvroString(V v, boolean singleLine) {
    try {
      if (v == null) return null;
      // ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.reset();
      Schema schema;
      if (GenericContainer.class.isAssignableFrom(v.getClass())) schema = ((GenericContainer) v)
          .getSchema();
      else schema = ReflectData.get().getSchema(v.getClass());
      if (schema == null) {
        return "null shcema can not be toString!";
      }
      JsonEncoder encoder;
      
      encoder = EncoderFactory.get().jsonEncoder(schema, out);
      // TODO Auto-generated catch block
      
      if (!singleLine) {
        JsonGenerator g = new JsonFactory().createJsonGenerator(out,
            JsonEncoding.UTF8);
        g.useDefaultPrettyPrinter();
        encoder.configure(g);
      }
      GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
      
      writer.write(v, encoder);
      encoder.flush();
      return new String(out.getData(), 0, out.getLength(), "utf-8");
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
