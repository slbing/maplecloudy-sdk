package com.maplecloudy.flume.sink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.hadoop.io.DataInputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maplecloudy.avro.util.AvroUtils;

@SuppressWarnings({"rawtypes"})
public class AvroFileSerializer {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFileSerializer.class);
  Schema schema;// = Schema.create(Schema.Type.LONG);
  DatumReader datumReader;
  
  GenericDatumWriter<Event> writer;
  
  public AvroFileSerializer() {
    // if (Pair.class.getName().equals(valueschema.getFullName())) {
    schema = ReflectData.get().getSchema(SimpleEvent.class);
    // } else {
    // schema = Pair.getPairSchema(Schema.create(Schema.Type.LONG),
    // valueschema);
    // }
    writer = new ReflectDatumWriter<Event>(schema);
    datumReader = GenericData.get().createDatumReader(schema);
  }
  
  public Schema getSchema() {
    return schema;
  }
  
  // public Schema getKeySchema() {
  // return schema.getField("key").schema();
  // // return keySchema;
  // }
  
  // public Schema getValueSchema() throws IOException {
  // return schema.getField("value").schema();
  // }
  
  /**
   * Format the given event into zero, one or more SequenceFile records
   * 
   * @param e
   *          event
   * @return a list of records corresponding to the given event
   */
  GenericData.Record obj;
  DataInputBuffer in = new DataInputBuffer();
  BinaryDecoder decoder;
  
  public GenericData.Record deSerialize(Event e) {
    decoder = DecoderFactory.get().binaryDecoder(e.getBody(), decoder);
    try {
      // in.reset(e.getBody(), e.getBody().length);
      obj = (GenericData.Record) datumReader.read(obj, decoder);
      
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    return obj;
  }
  
  ByteBufferOutputStream bos = new ByteBufferOutputStream();
  
  public List<ByteBuffer> serialize(Event e) throws IOException {
    try {
      BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bos,
          null);
      writer.write(e, encoder);
      return bos.getBufferList();
    } catch (Exception ex) {
      LOG.info("serialize event error:"+ AvroUtils.toString(e));
      LOG.info(ExceptionUtils.getFullStackTrace(ex));
      return new ArrayList<ByteBuffer>();
    }
  }
  
  public static void main(String[] args) {
    System.out.println(ReflectData.get().getSchema(SimpleEvent.class));
  }
}
