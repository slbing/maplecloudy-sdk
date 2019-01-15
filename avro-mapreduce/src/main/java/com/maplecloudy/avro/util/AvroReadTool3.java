package com.maplecloudy.avro.util;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.maplecloudy.avro.io.MapAvroFile;
import com.maplecloudy.avro.mapreduce.FsInput;

/** Reads a data file and dumps to JSON */
public class AvroReadTool3 implements Tool {
  
  @Override
  public String getName() {
    return "tojson2";
  }
  
  @Override
  public String getShortDescription() {
    return "Dumps an Avro data file as JSON, one record per line.";
  }
  
  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() < 1) {
      // Unlike other commands, "-" can't be used for stdin, because
      // we can only use seekable files.
      err.println("Usage: input_file [-num numToRead].");
      return 1;
    }
    int numToRead = 0;
    
   
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path inputFile = new Path(args.get(0));
    if (fs.isDirectory(inputFile)) {
      inputFile = new Path(inputFile, MapAvroFile.DATA_FILE_NAME);
    }
    
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
    FileReader<Object> fileReader = new DataFileReader<Object>(new FsInput(
        inputFile, conf), reader);
    try {
      Schema schema = fileReader.getSchema();
      DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
      Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
      for (Object datum : fileReader) {
        writer.write(datum, encoder);
        encoder.flush();
        numToRead++;
      }
      out.flush();
    } finally {
      fileReader.close();
      System.out.println("total:"+numToRead);
    }
    
    return 0;
  }
  
}
