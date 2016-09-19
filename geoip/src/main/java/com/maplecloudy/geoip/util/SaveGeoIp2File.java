package com.maplecloudy.geoip.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.avro.reflect.ReflectData;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.maplecloudy.avro.io.MapAvroFile;
import com.maplecloudy.avro.io.Pair;
import com.maplecloudy.geoip.model.AreaVo;
import com.maplecloudy.geoip.model.IpRange;
import com.maplecloudy.geoip.model.LocalId;

public class SaveGeoIp2File {
  public static final String GEOIP_STORE_FILE = "/user/sunflower/knowledge-db/geo-ip/avro";
  public static final String GEOIP_STORE_TXT_FILE = "/user/sunflower/knowledge-db/geo-ip/txt/ipRecord.txt";
  
  public static void save(TreeMap<IpRange,LocalId> mapIp,
      TreeMap<LocalId,AreaVo> mapLocal, Configuration conf) throws Exception {
    Path ipRange = new Path(GEOIP_STORE_FILE, "ip-range");
    FileSystem fs = ipRange.getFileSystem(conf);
    if (!fs.exists(ipRange)) {
      MapAvroFile.Writer<IpRange,LocalId> writer = new MapAvroFile.Writer<IpRange,LocalId>(
          conf, fs, ipRange.toString(), IpRange.schema, ReflectData.get()
              .getSchema(LocalId.class));
      for (Entry<IpRange,LocalId> entry : mapIp.entrySet()) {
        writer.append(entry.getKey(), entry.getValue());
      }
      writer.close();
    }
    Path areaVo = new Path(GEOIP_STORE_FILE, "area-vo");
    if (!fs.exists(areaVo)) {
      MapAvroFile.Writer<LocalId,AreaVo> writer = new MapAvroFile.Writer<LocalId,AreaVo>(
          conf, fs, areaVo.toString(), ReflectData.get().getSchema(
              LocalId.class), AreaVo.schema);
      
      for (Entry<LocalId,AreaVo> entry : mapLocal.entrySet()) {
        writer.append(entry.getKey(), entry.getValue());
      }
      writer.close();
    }
  }
  
  public static boolean loadFromFile(Map<IpRange,LocalId> mapIp,
      Map<LocalId,AreaVo> mapLocal, Map<AreaVo,LocalId> mapArea,
      Configuration conf) throws Exception {
    boolean bread = false;
    Path ipRange = new Path(SaveGeoIp2File.GEOIP_STORE_FILE, "ip-range");
    Path areaVo = new Path(SaveGeoIp2File.GEOIP_STORE_FILE, "area-vo");
    FileSystem fs = ipRange.getFileSystem(conf);
    if (fs.exists(ipRange)) {
      MapAvroFile.Reader<IpRange,LocalId> reader = null;
      try {
        reader = new MapAvroFile.Reader<IpRange,LocalId>(fs,
            ipRange.toString(), conf);
        while (reader.hasNext()) {
          Pair<IpRange,LocalId> pair = reader.next();
          mapIp.put(pair.key(), pair.value());
        }
        bread = true;
      } finally {
        if (reader != null) reader.close();
      }
    }
    
    if (fs.exists(areaVo)) {
      MapAvroFile.Reader<LocalId,AreaVo> reader = null;
      try {
        reader = new MapAvroFile.Reader<LocalId,AreaVo>(fs, areaVo.toString(),
            conf);
        while (reader.hasNext()) {
          Pair<LocalId,AreaVo> pair = reader.next();
          mapLocal.put(pair.key(), pair.value());
          mapArea.put(pair.value(), pair.key());
        }
        bread = true;
      } finally {
        if (reader != null) reader.close();
      }
    }
    return bread;
  }
  
  public static void loadFromTxtFile(Map<IpRange,LocalId> mapIp,
      Map<LocalId,AreaVo> mapLocal, Map<AreaVo,LocalId> mapArea,
      Configuration conf) throws Exception {
    Path txtIp = new Path(GEOIP_STORE_TXT_FILE);
    FileSystem fs = txtIp.getFileSystem(conf);
    // FSDataInputStream in = fs.open(txtIp);
    BufferedReader br = null;
    TreeSet<String> ts = new TreeSet<String>();
    
    try {
      br = new BufferedReader(new InputStreamReader(fs.open(txtIp)));
      String line = null;
      while ((line = br.readLine()) != null) {
        String[] arr = line.split(" ");
        if (arr.length < 7) {
          System.out.println("skip the bad line:" + line);
          continue;
        }
        // System.out.println("get line:" + line);
        IpRange ir = new IpRange(Long.valueOf(arr[0]), Long.valueOf(arr[1]));
        AreaVo av = new AreaVo(arr[2], arr[3], arr[4], arr[5]);
        LocalId ll = new LocalId(Integer.parseInt(arr[6]),
            Integer.parseInt(arr[7]), Integer.parseInt(arr[8]),
            Integer.parseInt(arr[9]));
        if (mapLocal.get(ll) != null && !av.equals(mapLocal.get(ll))) {
          // System.out.println(mapLocal.get(ll) +"+++"+av);
          if (av.compareTo(mapLocal.get(ll)) > 0) {
            ts.add(av.toString() + ":" + mapLocal.get(ll).toString());
          } else {
            ts.add(mapLocal.get(ll).toString() + ":" + av.toString());
          }
          
        }
        
        mapLocal.put(ll, av);
        mapArea.put(av, ll);
        // the same LocalId only keep one copy
        mapIp.put(ir, mapArea.get(av));
        
      }
      System.out.println(StringUtils.join(ts, "\n"));
      // System.out.println(tsv.size());
    } finally {
      if (br != null) br.close();
    }
  }
}
