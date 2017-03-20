package com.maplecloudy.avro.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import com.maplecloudy.avro.io.UnionData;
@SuppressWarnings("rawtypes")
public abstract class ConfigSchemaData extends Configured {
  // public Schema getSchema(Configuration conf);
  public final static String UNION_CLASS = "union.class";
  public static Schema getSchema(Configuration conf) {
    
    
    Class[] parseClass = null;
    if (conf != null) {
      parseClass = conf.getClasses(UNION_CLASS, null);
    }
    
    List<Schema> branches = new ArrayList<Schema>();
    branches.add(Schema.create(Schema.Type.NULL));
    if (parseClass != null) {
      for (Class branch : parseClass) {
        branches.add(ReflectData.get().getSchema(branch));
      }
      
    }
    Schema field = Schema.createUnion(branches);
    Schema schema = Schema.createRecord(UnionData.class.getName(), null, null,
        false);
    ArrayList<Field> fields = new ArrayList<Field>();
    fields.add(new Field("datum", field, null, null));
    schema.setFields(fields);
    return schema;
  }
}
