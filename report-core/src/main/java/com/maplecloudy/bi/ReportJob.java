package com.maplecloudy.bi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.maplecloudy.avro.mapreduce.AvroJob;

public class ReportJob {
  public static AvroJob get(Configuration conf) throws IOException
  {
    AvroJob job = AvroJob.getAvroJob(conf);
    job.setSortComparatorClass(ReportKeyComparator.class);
    return job;
  }
}
