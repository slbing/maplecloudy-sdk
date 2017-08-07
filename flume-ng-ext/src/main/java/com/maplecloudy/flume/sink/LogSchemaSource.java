package com.maplecloudy.flume.sink;

import java.io.IOException;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class LogSchemaSource {
  
  public static final String SCHEMA_FOLDER_PATH = "schema";
  public static final String SCHEMA_POSTFIX = ".avsc";
  
  private static final Logger logger = LoggerFactory
      .getLogger(LogSchemaSource.class);
  
  private boolean useRawLocalFileSystem;
  private HashMap<String,AvroFileSerializer> avroSerializers;
  
  private static LogSchemaSource mLogSchemaSource = null;
  
  public LogSchemaSource(Context context) {
    useRawLocalFileSystem = context.getBoolean("hdfs.useRawLocalFileSystem",
        false);
    avroSerializers = Maps.newHashMap();
  }
  
  public static LogSchemaSource getInstance(Context context) {
    if (mLogSchemaSource == null) {
      mLogSchemaSource = new LogSchemaSource(context);
    }
    return mLogSchemaSource;
  }
  
  public AvroFileSerializer getAvroSerializer(String schemaName, String ver)
      throws IOException {
    String schema = schemaName + "-" + ver;
    AvroFileSerializer avroSerializer = avroSerializers.get(schema);
    
    if (avroSerializer == null) {
      avroSerializer = new AvroFileSerializer(getvalueSchema(schemaName, ver));
      avroSerializers.put(schema, avroSerializer);
    }
    return avroSerializer;
  }
  
  private Schema getvalueSchema(String schemaName, String ver)
      throws IOException {
    Schema valueSchema = null;
    Configuration conf = new Configuration();
    Path dstPath = new Path(SCHEMA_FOLDER_PATH + "/" + schemaName + "-" + ver
        + SCHEMA_POSTFIX);
    FileSystem hdfs = dstPath.getFileSystem(conf);
    
    if (useRawLocalFileSystem) {
      if (hdfs instanceof LocalFileSystem) {
        hdfs = ((LocalFileSystem) hdfs).getRaw();
      } else {
        logger.warn("useRawLocalFileSystem is set to true but file system "
            + "is not of type LocalFileSystem: " + hdfs.getClass().getName());
      }
    }
    if (hdfs.exists(dstPath)) {
      valueSchema = new Schema.Parser().parse(hdfs.open(dstPath));
    } else {
      valueSchema = Schema.create(Schema.Type.STRING);
    }
    return valueSchema;
  }
  
}
