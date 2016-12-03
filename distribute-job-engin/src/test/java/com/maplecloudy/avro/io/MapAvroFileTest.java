package com.maplecloudy.avro.io;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.maplecloudy.distribute.engine.MapAvroFile;

public class MapAvroFileTest {
  
  public static class SeqValue {
    int iseq;
    String str1 = "问渠那得清如许，为有源头活水来。";
    String str2 = "问渠那得清如许，为有源头活水来。";
    String str3 = "问渠那得清如许，为有源头活水来。";
    String str4 = "问渠那得清如许，为有源头活水来。";
    long time = System.currentTimeMillis();
    
  }
  
  public void CreateAvroDataFile(Path path, int records) throws IOException,
      InstantiationException, IllegalAccessException {
    if (path == null) path = new Path("avro-test-data");
    Configuration conf = new Configuration();
    Schema keyschema = Schema.create(Schema.Type.STRING);
    Schema valueschema = ReflectData.get().getSchema(SeqValue.class);
    MapAvroFile.Writer<String,SeqValue> wr = new MapAvroFile.Writer<String,SeqValue>(
        conf, FileSystem.get(conf), "new", keyschema, valueschema);
    
    for (int i = 0; i < records; i++) {
      SeqValue val = new SeqValue();
      val.iseq = i;
      wr.append(shiftString(i), val);
    }
    wr.close();
  }
  
  private String shiftString(int i)
  {
    String src = i+"";
    while(src.length() < 10)
    {
      src = "0"+src;
    }
    return src;
  }
  
  public static void main(String args[]) throws Exception {
    MapAvroFileTest mf = new MapAvroFileTest();
    mf.CreateAvroDataFile(null, 1000000);
  }
}
