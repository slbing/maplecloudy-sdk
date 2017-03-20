package com.maplecloudy.avro.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.data.Json;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;

import com.maplecloudy.avro.io.MapAvroFile;
import com.maplecloudy.avro.mapreduce.FsInput;
import com.maplecloudy.avro.mapreduce.output.AvroMapOutputFormat;
import com.maplecloudy.avro.mapreduce.output.AvroPairOutputFormat;
import com.maplecloudy.avro.reflect.ReflectDataEx;

@SuppressWarnings({"rawtypes", "unchecked"})
public class AvroUtils {
  static DataOutputBuffer out = new DataOutputBuffer();
  static DataInputBuffer in = new DataInputBuffer();
  
  // static ByteArrayOutputStream arrout = new ByteArrayOutputStream();
  
  public static <V> V clone(V v) throws IOException {
    out.reset();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    Schema schema;
    if (GenericContainer.class.isAssignableFrom(v.getClass())) schema = ((GenericContainer) v)
        .getSchema();
    else schema = ReflectData.get().getSchema(v.getClass());
    
    GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
    GenericDatumReader<V> reader = new ReflectDatumReader<V>(schema);
    writer.write(v, encoder);
    in.reset(out.getData(), out.getLength());
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
    return reader.read(null, decoder);
    
  }
  
  public static <V> DataOutputBuffer serialize(V v) throws IOException {
    out.reset();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    Schema schema;
    if (GenericContainer.class.isAssignableFrom(v.getClass())) schema = ((GenericContainer) v)
        .getSchema();
    else schema = ReflectData.get().getSchema(v.getClass());
    
    GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
    writer.write(v, encoder);
    return out;
  }
  
  public static <V> String toString(V v) {
    StringBuilder buffer = new StringBuilder();
    Schema schema;
    if (GenericContainer.class.isAssignableFrom(v.getClass())) schema = ((GenericContainer) v)
        .getSchema();
    else schema = ReflectData.get().getSchema(v.getClass());
    toString(v, buffer, schema);
    
    return buffer.toString();
  }
  
  public static <V> String toAvroString(V v) {
    
    return toAvroString(v, true);
  }
  
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
  
  public static <V> JsonNode toJson(V v) throws IOException {
    out.reset();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    Schema schema;
    if (GenericContainer.class.isAssignableFrom(v.getClass())) schema = ((GenericContainer) v)
        .getSchema();
    else schema = ReflectData.get().getSchema(v.getClass());
    
    GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
    writer.write(v, encoder);
    in.reset(out.getData(), out.getLength());
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
    
    return Json.read(decoder);
  }
  
  protected static void toString(Object datum, StringBuilder buffer,
      Schema schema) {
    if (datum == null) {
      buffer.append("{null}");
      return;
    }
    if (schema.getType() == Schema.Type.RECORD) {
      buffer.append("{");
      int count = 0;
      
      for (Field f : schema.getFields()) {
        toString(f.name(), buffer, Schema.create(Schema.Type.STRING));
        buffer.append(": ");
        toString(ReflectData.get().getField(datum, f.name(), count), buffer,
            f.schema());
        // toString(record.get(f.pos()), buffer);
        if (++count < schema.getFields().size()) buffer.append(", ");
      }
      buffer.append("}");
    } else if (schema.getType() == Schema.Type.ARRAY) {
      
      if (datum instanceof Collection) {
        Collection<?> array = (Collection<?>) datum;
        buffer.append("[");
        long last = array.size() - 1;
        int i = 0;
        for (Object element : array) {
          toString(element, buffer, schema.getElementType());
          if (i++ < last) buffer.append(", ");
        }
        buffer.append("]");
      } else if (datum != null && datum.getClass().isArray()) {
        buffer.append("[");
        long last = ((Object[]) datum).length - 1;
        int i = 0;
        for (Object element : ((Object[]) datum)) {
          toString(element, buffer, schema.getElementType());
          if (i++ < last) buffer.append(", ");
        }
        buffer.append("]");
      }
    } else if (schema.getType() == Schema.Type.MAP) {
      buffer.append("{");
      int count = 0;
      Map<Object,Object> map = (Map<Object,Object>) datum;
      for (Map.Entry<Object,Object> entry : map.entrySet()) {
        toString(entry.getKey(), buffer, schema.getValueType());
        buffer.append(": ");
        toString(entry.getValue(), buffer, schema.getValueType());
        if (++count < map.size()) buffer.append(", ");
      }
      buffer.append("}");
    } else if (schema.getType() == Schema.Type.STRING) {
      buffer.append("\"");
      buffer.append(datum); // TODO: properly escape!
      buffer.append("\"");
    } else if (schema.getType() == Schema.Type.FIXED
        || schema.getType() == Schema.Type.BYTES) {
      if (datum instanceof ByteBuffer) {
        buffer.append("{\"bytes\": \"");
        ByteBuffer bytes = (ByteBuffer) datum;
        for (int i = bytes.position(); i < bytes.limit(); i++)
          buffer.append((char) bytes.get(i));
        buffer.append("\"}");
      } else buffer.append(datum);
    } else if (schema.getType() == Schema.Type.UNION) {
      int index = GenericData.get().resolveUnion(schema, datum);
      toString(datum, buffer, schema.getTypes().get(index));
    } else {
      buffer.append(datum);
    }
  }
  
  public static DataFileWriter Create(Configuration conf, Path path,
      Schema schema) throws IOException {
    DataFileWriter data = new DataFileWriter(new ReflectDatumWriter(
        (Schema) null, ReflectDataEx.get()));
    data.create(schema, path.getFileSystem(conf).create(path, null));
	  return data;
  }
  
  public static DataFileReader Open(Configuration conf, Path path)
      throws IOException {
	  DataFileReader data = new DataFileReader(new FsInput(path, conf),
			  new ReflectDatumReader(null,null,ReflectDataEx.get()));
	  return data;
  }
  
  public static long getRecordNum(Configuration conf, String inputPath,
      boolean isMap) throws Exception {
    return getRecordNum(conf, new Path(inputPath), isMap);
  }
  
  public static long getRecordNum(Configuration conf, Path inputPath,
      boolean isMap) throws Exception {
    
    conf.setClass(MapAvroFile.Reader.DATUM_READER_CLASS,
        GenericDatumReader.class, DatumReader.class);
    FileSystem fs = FileSystem.get(conf);
    int total = 0;
    if (isMap) {
      Path datafile = new Path(inputPath, MapAvroFile.DATA_FILE_NAME);
      Path indexfile = new Path(inputPath, MapAvroFile.INDEX_FILE_NAME);
      MapAvroFile.Reader[] readers = null;
      if (fs.exists(datafile) && fs.exists(indexfile)) {
        readers = new MapAvroFile.Reader[] {new MapAvroFile.Reader(fs,
            inputPath.toString(), conf)};
      } else readers = AvroMapOutputFormat.getReaders(inputPath, conf);
      try {
        for (MapAvroFile.Reader reader : readers) {
          total += reader.size();
        }
      } finally {
        for (int i = 0; i < readers.length; i++) {
          try {
            readers[i].close();
          } catch (Exception e) {
            
          }
        }
      }
    } else {
      DataFileReader[] readers = null;
      if (fs.isFile(inputPath)) {
        readers = new DataFileReader[] {AvroUtils.Open(conf, inputPath)};
      } else readers = AvroPairOutputFormat.getReaders(inputPath, conf);
      try {
        for (DataFileReader reader : readers) {
          while (reader.hasNext()) {
            reader.next();
            total++;
          }
        }
      } finally {
        for (int i = 0; i < readers.length; i++) {
          try {
            readers[i].close();
          } catch (Exception e) {
            
          }
        }
      }
    }
    return total;
  }
  
  public static void main(String[] args) throws Exception {
    HashMap<String,Integer> hm = new HashMap<String,Integer>();
    hm.put("1", 1);
    hm.put("2", 2);
    HashMap<String,Integer> tmp = AvroUtils.clone(hm);
    
    System.out.println(tmp);
  }
}
