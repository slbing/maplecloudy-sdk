package com.maplecloudy.flume.sink;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Event;
import org.apache.hadoop.io.DataInputBuffer;

@SuppressWarnings({"rawtypes"})
public class AvroFileSerializer {
  
  // Schema keySchema = Schema.create(Schema.Type.LONG), valueSchema;
  Schema schema;// = Schema.create(Schema.Type.LONG);
  DatumReader datumReader;
  String schemaPath;
  
  // public AvroFileSerializer(String schemapath){
  // schemaPath = schemapath;
  // }
  
  public AvroFileSerializer(Schema valueschema) {
    // if (Pair.class.getName().equals(valueschema.getFullName())) {
    schema = valueschema;
    // } else {
    // schema = Pair.getPairSchema(Schema.create(Schema.Type.LONG),
    // valueschema);
    // }
    datumReader = GenericData.get().createDatumReader(valueschema);
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
  
  public ByteBuffer serialize(Event e) {
    return ByteBuffer.wrap(e.getBody());
    
  }
  
}
