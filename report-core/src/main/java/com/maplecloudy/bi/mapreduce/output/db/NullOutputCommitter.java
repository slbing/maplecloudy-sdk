package com.maplecloudy.bi.mapreduce.output.db;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * OutputCommitter instance that does nothing.
 */
public class NullOutputCommitter extends OutputCommitter {

  public void abortTask(TaskAttemptContext taskContext) { }

  public void cleanupJob(JobContext jobContext) { }

  public void commitTask(TaskAttemptContext taskContext) { }

  public boolean needsTaskCommit(TaskAttemptContext taskContext) {
    return false;
  }

  public void setupJob(JobContext jobContext) { }

  public void setupTask(TaskAttemptContext taskContext) { }

}

