package com.maplecloudy.avro.util;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/** Reads a data file and dumps to JSON */
public class AvroSumTool implements Tool {
  
  @Override
  public String getName() {
    return "sum";
  }
  
  @Override
  public String getShortDescription() {
    return "Total number of records in an Avro data file .";
  }
  
  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() < 1) {
      err.println("Usage: input_file  [-f map/avro].");
      return 1;
    }
    boolean isMap = false;
    for (int i = 0; i < args.size(); i++) {
      if ("-f".equals(args.get(i))) {
        isMap = "map".equals(args.get(i + 1)) ? true : false;
        i++;
      }
    }
    
    System.out.println("record num:"
        + AvroUtils.getRecordNum(new Configuration(),args.get(0), isMap));
    return 0;
  }
  
}
