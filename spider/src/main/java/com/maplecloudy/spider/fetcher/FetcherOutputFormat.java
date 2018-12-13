package com.maplecloudy.spider.fetcher;

import java.io.IOException;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;

import com.google.gson.Gson;
import com.maplecloudy.avro.io.UnionData;
import com.maplecloudy.avro.mapreduce.output.AvroMapOutputFormat;
import com.maplecloudy.avro.mapreduce.output.AvroPairOutputFormat;
import com.maplecloudy.avro.mapreduce.output.MultipleOutputs;
import com.maplecloudy.spider.crawl.CrawlDatum;
import com.maplecloudy.spider.metadata.Spider;
import com.maplecloudy.spider.parse.Outlink;
import com.maplecloudy.spider.protocol.Content;

public class FetcherOutputFormat extends FileOutputFormat<String,UnionData> {
  
  @Override
  public RecordWriter<String,UnionData> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    
    final MultipleOutputs mos = new MultipleOutputs(job);
    Gson gson = new Gson();
    return new RecordWriter<String,UnionData>() {
      
      @Override
      public void write(String key, UnionData value)
          throws IOException, InterruptedException {
        
        if (value.datum instanceof CrawlDatum) {
          mos.write(AvroMapOutputFormat.class, CrawlDatum.FETCH_DIR_NAME + "/",
              key, value.datum);
        } else if (value.datum instanceof Content) {
          mos.write(AvroMapOutputFormat.class, FetcherSmart.CONTENT_REDIR + "/",
              key, value.datum);
        } else if (value.datum instanceof Outlink) {
          Outlink ol = (Outlink) value.datum;
          CrawlDatum datum = new CrawlDatum((int) CrawlDatum.STATUS_LINKED,
              ol.getFetchInterval());
          datum.setExtendData(ol.getExtend());
          mos.write(AvroPairOutputFormat.class, Spider.PARSE_DIR_NAME + "/",
              ol.url, datum);
        } else {
          try {
            mos.write(AvroMapOutputFormat.class,
                value.datum.getClass().getSimpleName() + "/", key, value.datum);
          } catch (Exception e) {
            Log.info(gson.toJson(value.datum));
          }
        }
      }
      
      @Override
      public void close(TaskAttemptContext context)
          throws IOException, InterruptedException {
        mos.close();
        
      }
    };
  }
  
  public void checkOutputSpecs(JobContext job)
      throws FileAlreadyExistsException, IOException {
    // Ensure that the output directory is set and not already there
    Path outDir = getOutputPath(job);
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set.");
    }
    
    // get delegation token for outDir's file system
    // TokenCache.obtainTokensForNamenodes(new Path[] { outDir },
    // job.getConfiguration());
    
    // if (outDir.getFileSystem(job.getConfiguration()).exists(outDir)) {
    // throw new FileAlreadyExistsException("Output directory " + outDir
    // + " already exists");
    // }
  }
}
