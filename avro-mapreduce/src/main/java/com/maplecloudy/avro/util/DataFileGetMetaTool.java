package com.maplecloudy.avro.util;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;

/** Reads a data file to get its metadata. */
public class DataFileGetMetaTool implements Tool {
  
  @Override
  public String getName() {
    return "getmeta";
  }
  
  @Override
  public String getShortDescription() {
    return "Prints out the metadata of an Avro data file.";
  }
  
  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    
    if (args.size() != 1) {
      err.println("Expected 1 arg: input_file");
      return 1;
    }
    DataFileReader<Void> reader = new DataFileReader<Void>(
        new File(args.get(0)), new GenericDatumReader<Void>());
    List<String> keys = reader.getMetaKeys();
    for (String key : keys) {
      out.print(escapeKey(key));
      out.print('\t');
      byte[] value = reader.getMeta(key);
      out.write(value, 0, value.length);
      out.println();
    }
    return 0;
  }
  
  // escape TAB, NL and CR in keys, so that output can be reliably parsed
  static String escapeKey(String key) {
    key = key.replace("\\", "\\\\"); // escape backslashes first
    key = key.replace("\t", "\\t"); // TAB
    key = key.replace("\n", "\\n"); // NL
    key = key.replace("\r", "\\r"); // CR
    return key;
  }
  
}
