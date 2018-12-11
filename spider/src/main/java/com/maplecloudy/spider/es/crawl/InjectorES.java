
package com.maplecloudy.spider.es.crawl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

import com.maplecloudy.avro.mapreduce.AvroJob;
import com.maplecloudy.avro.mapreduce.input.AvroPairInputFormat;
import com.maplecloudy.avro.mapreduce.output.AvroMapOutputFormat;
import com.maplecloudy.avro.mapreduce.output.AvroPairOutputFormat;
import com.maplecloudy.oozie.main.OozieMain;
import com.maplecloudy.oozie.main.OozieToolRunner;
import com.maplecloudy.spider.crawl.CrawlDatum;
import com.maplecloudy.spider.crawl.CrawlDb;
import com.maplecloudy.spider.crawl.CrawlDbFilter;
import com.maplecloudy.spider.net.BasicURLNormalizer;
import com.maplecloudy.spider.util.SpiderConfiguration;

/**
 * This class takes a flat file of URLs and adds them to the of pages to be
 * crawled. Useful for bootstrapping the system. The URL files contain one URL
 * per line, optionally followed by custom metadata separated by tabs with the
 * metadata key separated from the corresponding value by '='. <br>
 * Note that some metadata keys are reserved : <br>
 * - <i>nutch.score</i> : allows to set a custom score for a specific URL <br>
 * - <i>nutch.fetchInterval</i> : allows to set a custom fetch interval for a
 * specific URL <br>
 * e.g. http://www.nutch.org/ \t nutch.score=10 \t nutch.fetchInterval=2592000
 * \t userType=open_source
 **/
public class InjectorES extends OozieMain implements Tool {
  
  public static final Log LOG = LogFactory.getLog(InjectorES.class);
  
  /** Normalize and filter injected urls. */
  public static class InjectMapper
      extends Mapper<WritableComparable,Text,String,CrawlDatum> {
    
    private int interval;
    private float scoreInjected;
    private Configuration jobConf;
    private long curTime;
    
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      jobConf = context.getConfiguration();
      interval = jobConf.getInt("db.fetch.interval.default", 2592000);
      scoreInjected = jobConf.getFloat("db.score.injected", 1.0f);
      curTime = jobConf.getLong("injector.current.time",
          System.currentTimeMillis());
    }
    
    @Override
    protected void map(WritableComparable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      String url = value.toString();
      float customScore = -1f;
      int customInterval = interval;
      java.util.Map<String,String> metaData = new HashMap<String,String>();
      Map<String,String> extendData = new HashMap<String,String>();
      if (url.indexOf("\t") != -1) {
        String[] splits = url.split("\t");
        url = splits[0];
        for (int s = 1; s < splits.length; s++) {
          // find separation between name and value
          int indexEquals = splits[s].indexOf("=");
          if (indexEquals == -1) {
            // skip anything without a =
            continue;
          }
          String metaname = splits[s].substring(0, indexEquals);
          String metavalue = splits[s].substring(indexEquals + 1);
          if (!metaname.equals(CrawlDatum.PARSE_CLASS)) {
            metaData.put(metaname, metavalue);
          } else extendData.put(metaname, metavalue);
        }
        try {
          url = new BasicURLNormalizer().normalize(url);
        } catch (MalformedURLException e) {
          LOG.warn("Skipping " + url + ":" + e);
          url = null;
        }
        if (url != null) { // if it passes
          CrawlDatum datum = new CrawlDatum(CrawlDatum.STATUS_INJECTED,
              customInterval);
          datum.setFetchTime(curTime);
          if (customScore != -1) datum.setScore(customScore);
          else {
            datum.setScore(scoreInjected);
          }
          datum.setMetaData(metaData);
          datum.setExtendData(extendData);
          context.write(url, datum);
        }
      }
    }
    
  }
  
  /** Combine multiple new entries for a url. */
  public static class InjectReducer
      extends Reducer<String,CrawlDatum,String,CrawlDatum> {
    
    private CrawlDatum old = new CrawlDatum();
    private CrawlDatum injected = new CrawlDatum();
    
    @Override
    protected void reduce(String key, Iterable<CrawlDatum> values,
        Context context) throws IOException, InterruptedException {
      boolean oldSet = false;
      for (CrawlDatum cur : values) {
        
        if (cur.getStatus() == CrawlDatum.STATUS_INJECTED) {
          injected.set(cur);
          injected.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
        } else {
          old.set(cur);
          oldSet = true;
        }
      }
      CrawlDatum res = null;
      if (oldSet) res = old; // don't overwrite existing value
      else res = injected;
      context.write(key, res);
    }
  }
  
  public InjectorES() {}
  
  public InjectorES(Configuration conf) {
    setConf(conf);
  }
  
  public int inject(Path crawlDb, Path... urlDirs) throws IOException,
      ClassNotFoundException, IllegalStateException, InterruptedException {
    
    if (LOG.isInfoEnabled()) {
      LOG.info("Injector: starting");
      LOG.info("Injector: crawlDb: " + crawlDb);
      LOG.info("Injector: urlDir: " + urlDirs);
    }
    
    Path tempDir = new Path(
        getConf().get("mapred.temp.dir", ".") + "/inject-temp-"
            + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
    
    // map text input file to a <url,CrawlDatum> file
    if (LOG.isInfoEnabled()) {
      LOG.info("Injector: Converting injected urls to crawl db entries.");
    }
    AvroJob sortJob = AvroJob.getAvroJob(getConf());
    sortJob.setJobName("inject " + urlDirs);
    
    for (Path urlDir : urlDirs) {
      FileInputFormat.addInputPath(sortJob, urlDir);
    }
    
    sortJob.setMapperClass(InjectMapper.class);
    FileOutputFormat.setOutputPath(sortJob, tempDir);
    sortJob.setOutputKeyClass(String.class);
    sortJob.setOutputValueClass(CrawlDatum.class);
    sortJob.setOutputFormatClass(AvroPairOutputFormat.class);
    if (runJob(sortJob)) {
      // merge with existing crawl db
      if (LOG.isInfoEnabled()) {
        LOG.info("Injector: Merging injected urls into crawl db.");
      }
      AvroJob job = AvroJob.getAvroJob(getConf());
      job.setJobName("crawldb " + crawlDb);
      
      Path current = new Path(crawlDb, CrawlDb.CURRENT_NAME);
      FileInputFormat.addInputPath(job, tempDir);
      if (FileSystem.get(getConf()).exists(current)) {
        FileInputFormat.addInputPath(job, current);
      }
      job.setInputFormatClass(AvroPairInputFormat.class);
      job.setReducerClass(InjectReducer.class);
      job.setMapperClass(CrawlDbFilter.class);
      Path newCrawlDb = new Path(crawlDb,
          Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
      FileOutputFormat.setOutputPath(job, newCrawlDb);
      job.setOutputFormatClass(AvroMapOutputFormat.class);
      job.setOutputKeyClass(String.class);
      job.setOutputValueClass(CrawlDatum.class);
      if (runJob(job)) {
        CrawlDb.install(job, crawlDb);
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(tempDir, true);
        if (LOG.isInfoEnabled()) {
          LOG.info("Injector: done");
        }
        return 0;
      } else {
        return -1;
      }
      
    } else {
      return -1;
    }
  }
  
  public static void main(String[] args) throws Exception {
    int res = OozieToolRunner.run(SpiderConfiguration.create(), new InjectorES(),
        args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: Injector <crawldb> <url_dir>");
      return -1;
    }
    try {
      inject(new Path(args[0]), new Path(args[1]));
      return 0;
    } catch (Exception e) {
      LOG.fatal("Injector: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
  
}
