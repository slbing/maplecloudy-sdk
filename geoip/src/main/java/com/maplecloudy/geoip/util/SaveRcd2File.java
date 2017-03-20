package com.maplecloudy.geoip.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.maplecloudy.avro.io.MapAvroFile;
import com.maplecloudy.avro.io.Pair;
import com.maplecloudy.geoip.constant.Constants;

public class SaveRcd2File {
  public static void save(String subName, TreeMap<String,Integer> mapAue,
      Configuration conf) throws Exception {
    Path p = new Path(Constants.GEO_STORE_ROOT, subName + "/"
        + Constants.GEO_STORE_SUB_AVRO);
    FileSystem fs = p.getFileSystem(conf);
    if (!fs.exists(p)) {
      MapAvroFile.Writer<String,Integer> writer = new MapAvroFile.Writer<String,Integer>(
          conf, fs, p.toString(), ReflectData.get().getSchema(String.class),
          ReflectData.get().getSchema(Integer.class));
      for (Entry<String,Integer> entry : mapAue.entrySet())
        writer.append(entry.getKey(), entry.getValue());
      writer.close();
    }
  }
  
  public static boolean loadFromFile(String subName,
      TreeMap<String,Integer> mapAue, Configuration conf) throws Exception {
    boolean bread = false;
    Path p = new Path(Constants.GEO_STORE_ROOT, subName + "/"
        + Constants.GEO_STORE_SUB_AVRO);
    FileSystem fs = p.getFileSystem(conf);
    if (fs.exists(p)) {
      MapAvroFile.Reader<String,Integer> reader = null;
      try {
        reader = new MapAvroFile.Reader<String,Integer>(fs, p.toString(), conf);
        while (reader.hasNext()) {
          Pair<String,Integer> pair = reader.next();
          mapAue.put(pair.key(), pair.value());
        }
        bread = true;
      } finally {
        if (reader != null) reader.close();
      }
    }
    return bread;
  }
  
  public static void loadFromTxtFile(String subName,
      TreeMap<String,Integer> mapAue, Configuration conf) throws Exception {
    Path p = new Path(Constants.GEO_STORE_ROOT, subName + "/"
        + Constants.GEO_STORE_SUB_TXT);
    FileSystem fs = p.getFileSystem(conf);
    BufferedReader br = null;
    
    try {
      br = new BufferedReader(new InputStreamReader(fs.open(p)));
      String line = null;
      String value = null;
      int idx = 0;
      while ((line = br.readLine()) != null) {
        if (line.length() == 0) continue;
        
        value = line.trim().toLowerCase();
        mapAue.put(value, Integer.valueOf(idx++));
      }
    } finally {
      if (br != null) br.close();
    }
  }
}