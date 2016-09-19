package com.maplecloudy.bi.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.maplecloudy.avro.io.UnionData;
import com.maplecloudy.avro.mapreduce.output.AvroMapOutputFormat;
import com.maplecloudy.avro.mapreduce.output.MultipleOutputs;

public class PreProcessLogOutput extends FileOutputFormat<String,UnionData> {
  
  @Override
  public RecordWriter<String,UnionData> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    final MultipleOutputs mos = new MultipleOutputs(job);
    return new RecordWriter<String,UnionData>() {
      
      @Override
      public void write(String key, UnionData value) throws IOException,
          InterruptedException {
        mos.write(AvroMapOutputFormat.class,value.datum.getClass().getSimpleName(),
            key, value.datum);
      }
      
      @Override
      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {
        mos.close();
      }
    };
  }
}
