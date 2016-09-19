package com.maplecloudy.indexer.writer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public interface IndexWriter<V> {
  public void open(TaskAttemptContext job,Path out) throws Exception;
  
  public void open(TaskAttemptContext job,Path out,String local,String startShard,String endShard) throws Exception;
 
  public void open(TaskAttemptContext job,Path out,String local ) throws Exception;
  
  public void write(V data) throws Exception;

  public void close() throws Exception;

}
