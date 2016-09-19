package com.maplecloudy.avro.util;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.maplecloudy.avro.io.MapAvroFile;
import com.maplecloudy.avro.mapreduce.FsInput;

/** Reads a data file to get its schema. */
public class GetSchemaTool implements Tool {
  
  @Override
  public String getName() {
    return "getschema";
  }
  
  @Override
  public String getShortDescription() {
    return "Prints out schema of an Avro data file.";
  }
  
  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() != 1) {
      err.println("Usage: input_file");
      return 1;
    }
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path inpitFile = new Path(args.get(0));
    if (fs.isDirectory(inpitFile)) {
      inpitFile = new Path(inpitFile, MapAvroFile.DATA_FILE_NAME);
    }
    DataFileReader<Void> reader = new DataFileReader<Void>(new FsInput(
        inpitFile, conf), new GenericDatumReader<Void>());
    try {
      
      out.println(reader.getSchema().toString(true));
      
    } finally {
      reader.close();
    }
    return 0;
  }
}
