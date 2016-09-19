package com.maplecloudy.bi.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.maplecloudy.avro.io.UnionData;
import com.maplecloudy.avro.mapreduce.output.AvroMapOutputFormat;
import com.maplecloudy.avro.mapreduce.output.MultipleOutputs;
import com.maplecloudy.avro.util.AvroUtils;
import com.maplecloudy.bi.mapreduce.output.db.ReportDbOutputFormat;
import com.maplecloudy.bi.model.report.ReportKey;
import com.maplecloudy.bi.model.report.ReportValues;
import com.maplecloudy.bi.report.algorithm.DbOutputAble;
import com.maplecloudy.bi.report.algorithm.ReportAlgorithm;

@SuppressWarnings("unchecked")
public class ReportOutputFormat extends
    FileOutputFormat<UnionData,ReportValues> {
  
  @Override
  public RecordWriter<UnionData,ReportValues> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {
    final MultipleOutputs mos = new MultipleOutputs(job);
    final Configuration conf = job.getConfiguration();
    return new RecordWriter<UnionData,ReportValues>() {
      
      @Override
      public void write(UnionData key, ReportValues value) throws IOException,
          InterruptedException {
        Class<?> ra = ReportAlgorithm.getAlgorithm(conf,
            (Class<? extends ReportKey>) key.datum.getClass());
        mos.write(AvroMapOutputFormat.class, ra.getSimpleName(), key, value);
//        if (!conf.getBoolean("report.process.actively",
//            false)) return;
//        if (DbOutputAble.class.isAssignableFrom(ra)) mos.write(
//            ReportDbOutputFormat.class, "dbUpdateOut", AvroUtils.clone(key.datum), AvroUtils.clone(value));
      }
      
      @Override
      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {
        mos.close();
      }
    };
  }
}
