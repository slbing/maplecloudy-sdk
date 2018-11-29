package com.maplecloudy.flume.sink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maplecloudy.avro.io.MapAvroFile;
import com.maplecloudy.avro.io.Pair;

@SuppressWarnings("rawtypes")
public class HDFSAvroFile implements HDFSWriter {
  
  private static final Logger logger = LoggerFactory
      .getLogger(HDFSAvroFile.class);
  
  private DataFileWriter<Pair> writer;
  private String writeFormat;
  private AvroFileSerializer serializer;
  private boolean useRawLocalFileSystem;
  private Context mContext;
  
  public HDFSAvroFile() {
    writer = null;
  }
  
  @Override
  public void configure(Context context) {
    // use binary writable serialize by default
    
    mContext = context;
    writeFormat = context.getString("hdfs.writeFormat",
        SequenceFileSerializerType.Writable.name());
    useRawLocalFileSystem = context.getBoolean("hdfs.useRawLocalFileSystem",
        false);
    logger.info("writeFormat = " + writeFormat + ", UseRawLocalFileSystem = "
        + useRawLocalFileSystem);
    
  }
  
  @Override
  public void open(String filePath) throws IOException, ClassNotFoundException {
    open(filePath, null, CompressionType.NONE);
  }
  
  @Override
  public void open(String filePath, CompressionCodec codeC,
      CompressionType compType) throws IOException, ClassNotFoundException {
    open(filePath, codeC, compType, null);
  }
  
  @Override
  public void append(Event event) throws IOException {
    List<ByteBuffer> bfs = serializer.serialize(event);
    for (ByteBuffer bf : bfs) {
      writer.appendEncoded(bf);
    }
  }
  
  @Override
  public void sync() throws IOException {
    writer.flush();
  }
  
  @Override
  public void close() throws IOException {
    writer.close();
  }
  
  @Override
  public void open(String filePath, Map<String,String> head)
      throws IOException, ClassNotFoundException {
    open(filePath, null, CompressionType.NONE, head);
  }
  
  @Override
  public void open(String filePath, CompressionCodec codec,
      CompressionType cType, Map<String,String> head)
      throws IOException, ClassNotFoundException {
    Configuration conf = new Configuration();
    Path dstPath = new Path(filePath);
    FileSystem hdfs = dstPath.getFileSystem(conf);
    if (useRawLocalFileSystem) {
      if (hdfs instanceof LocalFileSystem) {
        hdfs = ((LocalFileSystem) hdfs).getRaw();
      } else {
        logger.warn("useRawLocalFileSystem is set to true but file system "
            + "is not of type LocalFileSystem: " + hdfs.getClass().getName());
      }
    }
    serializer = LogSchemaSource.getInstance(mContext).getAvroSerializer();
    
    writer = new DataFileWriter<Pair>(new ReflectDatumWriter<Pair>());
    writer
        .setCodec(CodecFactory.deflateCodec(MapAvroFile.DEFAULT_DEFLATE_LEVEL));
    // data.setCodec(CodecFactory.snappyCodec());
    // if (conf.getBoolean("hdfs.append.support", false) == true
    // && hdfs.isFile(dstPath)) {
    // FSDataOutputStream outStream = hdfs.append(dstPath);
    // writer.create(serializer.getSchema(), outStream);
    // } else {
    writer.create(serializer.getSchema(), hdfs.create(dstPath));
    // }
  }
  
}
