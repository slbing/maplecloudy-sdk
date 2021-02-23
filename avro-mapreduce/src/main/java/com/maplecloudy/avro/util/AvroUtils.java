package com.maplecloudy.avro.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
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

import com.google.common.collect.Maps;
import com.maplecloudy.avro.io.MapAvroFile;
import com.maplecloudy.avro.mapreduce.FsInput;
import com.maplecloudy.avro.mapreduce.output.AvroMapOutputFormat;
import com.maplecloudy.avro.mapreduce.output.AvroPairOutputFormat;
import com.maplecloudy.avro.reflect.ReflectDataEx;

@SuppressWarnings({"rawtypes", "unchecked"})
public class AvroUtils {
  
  static ThreadLocal<DataOutputBuffer> out = new ThreadLocal<DataOutputBuffer>() {
    protected synchronized DataOutputBuffer initialValue() {
      return new DataOutputBuffer();
    }
  };
  
  static ThreadLocal<DataInputBuffer> in = new ThreadLocal<DataInputBuffer>() {
    protected synchronized DataInputBuffer initialValue() {
      return new DataInputBuffer();
    }
  };
  
  public static <V> V clone(V v) throws IOException {
    out.get().reset();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out.get(),
        null);
    Schema schema;
    if (GenericContainer.class.isAssignableFrom(v.getClass()))
      schema = ((GenericContainer) v).getSchema();
    else schema = ReflectData.get().getSchema(v.getClass());
    
    GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
    GenericDatumReader<V> reader = new ReflectDatumReader<V>(schema);
    writer.write(v, encoder);
    in.get().reset(out.get().getData(), out.get().getLength());
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in.get(),
        null);
    return reader.read(null, decoder);
    
  }
  
  public static <V> DataOutputBuffer serialize(V v) throws IOException {
    out.get().reset();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out.get(),
        null);
    Schema schema;
    if (GenericContainer.class.isAssignableFrom(v.getClass()))
      schema = ((GenericContainer) v).getSchema();
    else {
      schema = schemas.get(v.getClass());
      if (schema == null) {
        schema = ReflectData.get().getSchema(v.getClass());
        schemas.put(v.getClass(), schema);
      }
    }
    
    GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
    writer.write(v, encoder);
    return out.get();
  }
  
  static Map<Class<?>,Schema> schemas = Maps.newHashMap();
  static BinaryDecoder decoder;
  
  public static <V> V deSerialize(byte[] content, V v) throws IOException {
    decoder = DecoderFactory.get().binaryDecoder(content, decoder);
    Schema schema = schemas.get(v.getClass());
    if (schema == null) {
      schema = ReflectData.get().getSchema(v.getClass());
      schemas.put(v.getClass(), schema);
    }
    
    GenericDatumReader<V> reader = new ReflectDatumReader<V>(schema);
    return reader.read(v, decoder);
    
  }
  
  public static <V> String toString(V v) {
    StringBuilder buffer = new StringBuilder();
    Schema schema;
    if (GenericContainer.class.isAssignableFrom(v.getClass()))
      schema = ((GenericContainer) v).getSchema();
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
      out.get().reset();
      Schema schema;
      if (GenericContainer.class.isAssignableFrom(v.getClass()))
        schema = ((GenericContainer) v).getSchema();
      else schema = ReflectData.get().getSchema(v.getClass());
      if (schema == null) {
        return "null shcema can not be toString!";
      }
      JsonEncoder encoder;
      
      encoder = EncoderFactory.get().jsonEncoder(schema, out.get());
      GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
      
      writer.write(v, encoder);
      encoder.flush();
      return new String(out.get().getData(), 0, out.get().getLength(), "utf-8");
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
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
    DataFileWriter data = new DataFileWriter(
        new ReflectDatumWriter((Schema) null, ReflectDataEx.get()));
    data.create(schema, path.getFileSystem(conf).create(path, null));
    return data;
  }
  
  public static DataFileReader Open(Configuration conf, Path path)
      throws IOException {
    DataFileReader data = new DataFileReader(new FsInput(path, conf),
        new ReflectDatumReader(null, null, ReflectDataEx.get()));
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
        readers = new MapAvroFile.Reader[] {
            new MapAvroFile.Reader(fs, inputPath.toString(), conf)};
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
  
  /**
   * if keepNull is true,then we will keep null value; otherwise,do not keep.
   * 
   * @param record
   * @param keepNull
   * @return
   */
  public static Object toObject(GenericData.Record record, boolean keepNull) {
    return deepCopy(record.getSchema(), record, keepNull);
  }
  
  public static Object toObject(GenericData.Record record) {
    return deepCopy(record.getSchema(), record, false);
  }
  
  public static Object deepCopy(Schema schema, Object value, boolean keepNull) {
    if (value == null) {
      return null;
    }
    switch (schema.getType()) {
      case ARRAY:
        List<Object> arrayValue = (List) value;
        List<Object> arrayCopy = new ArrayList<>(arrayValue.size());
        for (Object obj : arrayValue) {
          Object deepCopy = deepCopy(schema.getElementType(), obj, keepNull);
          if (keepNull) {
            arrayCopy.add(deepCopy);
          } else {
            if (deepCopy != null) {
              arrayCopy.add(deepCopy);
            }
          }
        }
        return arrayCopy;
      case BOOLEAN:
        return value; // immutable
      case BYTES:
        ByteBuffer byteBufferValue = (ByteBuffer) value;
        int start = byteBufferValue.position();
        int length = byteBufferValue.limit() - start;
        byte[] bytesCopy = new byte[length];
        byteBufferValue.get(bytesCopy, 0, length);
        byteBufferValue.position(start);
        return DatatypeConverter.printBase64Binary(bytesCopy);
      case DOUBLE:
        return value; // immutable
      case ENUM:
        // Enums are immutable; shallow copy will suffice
        return value;
      case FIXED:
        return DatatypeConverter
            .printBase64Binary(((GenericFixed) value).bytes());
      case FLOAT:
        return value; // immutable
      case INT:
        return value; // immutable
      case LONG:
        return value; // immutable
      case MAP:
        Map<CharSequence,Object> mapValue = (Map) value;
        Map<CharSequence,Object> mapCopy = new HashMap<CharSequence,Object>(
            mapValue.size());
        for (Map.Entry<CharSequence,Object> entry : mapValue.entrySet()) {
          mapCopy.put(entry.getKey(),
              deepCopy(schema.getValueType(), entry.getValue(), keepNull));
        }
        return mapCopy;
      case NULL:
        return null;
      case RECORD:
        GenericData.Record oldState = (GenericData.Record) value;
        Map<CharSequence,Object> mapObj = new HashMap<CharSequence,Object>();
        for (Field f : schema.getFields()) {
          Object deepCopy = deepCopy(f.schema(), oldState.get(f.name()),
              keepNull);
          if (keepNull) {
            mapObj.put(f.name(), deepCopy);
          } else {
            if (deepCopy != null) {
              mapObj.put(f.name(), deepCopy);
            }
          }
        }
        return mapObj;
      case STRING:
        return value.toString();
      case UNION:
        return deepCopy(
            schema.getTypes()
                .get(GenericData.get().resolveUnion(schema, value)),
            value, keepNull);
      default:
        throw new AvroRuntimeException("Deep copy failed for schema \"" + schema
            + "\" and value \"" + value + "\"");
    }
  }
  
  public static void main(String[] args) throws Exception {
    HashMap<String,Integer> hm = new HashMap<String,Integer>();
    hm.put("1", 1);
    hm.put("2", 2);
    HashMap<String,Integer> tmp = AvroUtils.clone(hm);
    
    System.out.println(tmp);
  }
}
