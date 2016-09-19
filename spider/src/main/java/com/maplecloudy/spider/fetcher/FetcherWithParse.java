package com.maplecloudy.spider.fetcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.maplecloudy.avro.io.UnionData;
import com.maplecloudy.avro.mapreduce.AvroJob;
import com.maplecloudy.avro.mapreduce.MultithreadedBlockMapper;
import com.maplecloudy.avro.mapreduce.MultithreadedBlockMapper.BlockMapper;
import com.maplecloudy.avro.mapreduce.input.AvroPairInputFormat;
import com.maplecloudy.spider.crawl.CrawlDatum;
import com.maplecloudy.spider.metadata.Spider;
import com.maplecloudy.spider.parse.Outlink;
import com.maplecloudy.spider.parse.Parse;
import com.maplecloudy.spider.parse.ParserFactory;
import com.maplecloudy.spider.protocol.Content;
import com.maplecloudy.spider.protocol.Protocol;
import com.maplecloudy.spider.protocol.ProtocolFactory;
import com.maplecloudy.spider.protocol.ProtocolOutput;
import com.maplecloudy.spider.protocol.ProtocolStatus;
import com.maplecloudy.spider.util.LogUtil;
import com.maplecloudy.spider.util.SpiderConfiguration;

public class FetcherWithParse extends Configured implements Tool {
  
  public static final Log LOG = LogFactory.getLog(FetcherWithParse.class);
  
  public static final int PERM_REFRESH_TIME = 5;
  
  public static final String CONTENT_REDIR = "content";
  
  public static final String PROTOCOL_REDIR = "protocol";
  
  public FetcherWithParse(Configuration conf) {
    this.setConf(conf);
  }
  
  public static class InputFormat extends
      AvroPairInputFormat<String,CrawlDatum> {
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
      // generate splits
      List<InputSplit> splits = new ArrayList<InputSplit>();
      List<FileStatus> files = listStatus(job);
      for (FileStatus file : files) {
        splits.add(new FileSplit(file.getPath(), 0, file.getLen(),
            (String[]) null));
      }
      // Save the number of input files for metrics/loadgen
      // job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
      LOG.debug("Total # of splits: " + splits.size());
      return splits;
    }
  }
  
  public static boolean isParsing(Configuration conf) {
    return conf.getBoolean("fetcher.parse", false);
  }
  
  public static void setIsParseing(Configuration conf, boolean parseing) {
    conf.setBoolean("fetcher.parse", parseing);
  }
  
  public static boolean isStoringContent(Configuration conf) {
    return conf.getBoolean("fetcher.store.content", true);
  }
  
  public static class FetchMapper extends
      BlockMapper<String,CrawlDatum,String,UnionData> {
    
    private ProtocolFactory protocolFactory;
    Context outer;
    private String segmentName;
    private long start = System.currentTimeMillis(); // start time of
    // fetcher
    private boolean storingContent;
    private boolean parsing;
    
    protected void setup(Context context) throws IOException,
        InterruptedException {
      this.protocolFactory = new ProtocolFactory(context.getConfiguration());
      outer = context;
      this.segmentName = context.getConfiguration()
          .get(Spider.SEGMENT_NAME_KEY);
      storingContent = FetcherWithParse.isStoringContent(context
          .getConfiguration());
      parsing = FetcherWithParse.isParsing(context.getConfiguration());
    }
    
    @Override
    protected void map(String key, CrawlDatum value, Context context)
        throws IOException, InterruptedException {
      // url may be changed through redirects.
      // CrawlDatum value = new CrawlDatum(val);
      try {
        if (LOG.isInfoEnabled()) {
          LOG.info("fetching " + key);
        }
        
        Protocol protocol = this.protocolFactory.getProtocol(key);
        ProtocolOutput output = protocol.getProtocolOutput(key, value);
        ProtocolStatus status = output.getStatus();
        Content content = output.getContent();
        
        switch (status.getCode()) {
        
          case ProtocolStatus.SUCCESS: // got a page
            output(key, value, content, CrawlDatum.STATUS_FETCH_SUCCESS);
            updateStatus(content.getContent().length);
            
            break;
          
          default:
            if (LOG.isWarnEnabled()) {
              LOG.warn("ProtocolStatus: " + status.getName());
            }
            output(key, value, null, CrawlDatum.STATUS_FETCH_RETRY);
            logError(key.toString(), "" + status.getName());
        }
        
      } catch (Throwable t) { // unexpected exception
        logError(key.toString(), t.toString());
        t.printStackTrace();
        output(key, value, null, CrawlDatum.STATUS_FETCH_RETRY);
        
      }
    }
    
    // private long lastlogtime = 0;
    private void updateStatus(int bytesInPage) throws IOException {
      pages++;
      bytes += bytesInPage;
    }
    
    private void logError(String url, String message) {
      if (LOG.isInfoEnabled()) {
        LOG.info("fetch of " + url + " failed with: " + message);
      }
      
      errors++;
    }
    
    @Override
    public void BlockRecord() throws InterruptedException {
      if (currentValue != null) output(currentKey, currentValue, null,
          CrawlDatum.STATUS_FETCH_RETRY);
    }
    
    @SuppressWarnings({"deprecation"})
    private void output(String key, CrawlDatum datum, Content content,
        int status) throws InterruptedException {
      
      datum.setStatus(status);
      datum.setFetchTime(System.currentTimeMillis());
      
      if (content != null) {
        
        content.addMetadata(Spider.SEGMENT_NAME_KEY, segmentName);
        content.addMetadata(Spider.FETCH_STATUS_KEY, String.valueOf(status));
        content.setExtendData(datum.getExtendData());
      }
      
      try {
        outer.write(key, new UnionData(datum));
        
        if (content != null && storingContent) outer.write(key, new UnionData(
            content));
        if (content != null && parsing) {
          Parse parse = new ParserFactory().getParsers(key, content);
          try {
            @SuppressWarnings("rawtypes")
            List pd = parse.parse(key, content);
            for (Object o : pd) {
              if (o instanceof Outlink) {
                ((Outlink) o).setExtend(content.getExtendData());
                outer.write(key, new UnionData(((Outlink) o)));
              } else outer.write(key, new UnionData(o));
            }
          } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Parsing error:" + key);
            LOG.error("Parsing error:" + StringUtils.stringifyException(e));
          }
        }
        
      } catch (IOException e) {
        if (LOG.isFatalEnabled()) {
          e.printStackTrace(LogUtil.getFatalStream(LOG));
          LOG.fatal("fetcher caught:" + e.toString());
        }
      }
    }
  }
  
  /** Run the fetcher. */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(SpiderConfiguration.create(),
        new FetcherWithParse(SpiderConfiguration.create()), args);
    System.exit(res);
  }
  
  public boolean fetch(Path segment) throws IOException, InterruptedException,
      ClassNotFoundException {
    int threads = getConf().getInt("fetcher.threads.fetch", 10);
    return fetch(segment, threads);
  }
  
  public boolean fetch(Path segment, int threads) throws IOException,
      InterruptedException, ClassNotFoundException {
    
    if (LOG.isInfoEnabled()) {
      LOG.info("FetcherSmart: starting");
      LOG.info("FetcherSmart: segment: " + segment);
    }
    
    AvroJob job = AvroJob.getAvroJob(getConf());
    job.setJobName("fetch " + segment);
    
    job.getConfiguration().setInt("fetcher.threads.fetch", threads);
    job.getConfiguration().set(Spider.SEGMENT_NAME_KEY, segment.getName());
    
    // for politeness, don't permit parallel execution of a single task
    job.setSpeculativeExecution(false);
    
    FileInputFormat.addInputPath(job, new Path(segment,
        CrawlDatum.GENERATE_DIR_NAME));
    job.setInputFormatClass(InputFormat.class);
    job.setMapperClass(MultithreadedBlockMapper.class);
    // job.setReducerClass(MOReduce.class);
    MultithreadedBlockMapper.setMapperClass(job, FetchMapper.class);
    MultithreadedBlockMapper.setNumberOfThreads(job, threads);
    
    FileOutputFormat.setOutputPath(job, segment);
    job.setMapOutputKeyClass(String.class);
    job.setMapOutputValueClass(UnionData.class);
    job.setOutputFormatClass(FetcherOutputFormat.class);
    boolean ret = job.waitForCompletion(true);
    if (LOG.isInfoEnabled()) {
      LOG.info("FetcherSmart: done");
    }
    return ret;
  }
  
  public static class MOReduce extends Reducer<String,UnionData,String,Object> {
    
    @Override
    public void setup(Context context) {}
    
    @Override
    protected void reduce(String key, Iterable<UnionData> values,
        Context context) throws IOException, InterruptedException {
      for (UnionData value : values) {
        context.write(key, value);
      }
    }
    
  }
  
  public int run(String[] args) throws Exception {
    
    String usage = "Usage: Fetcher <segment> [-threads n]";
    
    if (args.length < 1) {
      System.err.println(usage);
      return -1;
    }
    
    Path segments = new Path(args[0]);
    
    int threads = getConf().getInt("fetcher.threads.fetch", 10);
    
    for (int i = 1; i < args.length; i++) { // parse command line
      if (args[i].equals("-threads")) { // found -threads option
        threads = Integer.parseInt(args[++i]);
      }
    }
    
    getConf().setInt("fetcher.threads.fetch", threads);
    FileSystem fs = FileSystem.get(getConf());
    try {
      for (FileStatus p : fs.listStatus(segments)) {
        if (fs.exists(new Path(p.getPath(), "generatored"))
            && !fs.exists(new Path(p.getPath(), "fetched"))) {
          fetch(p.getPath(), threads);
          fs.createNewFile(new Path(p.getPath(), "fetched"));
          // break;
        }
      }
      return 0;
    } catch (Exception e) {
      LOG.fatal("Fetcher: " + StringUtils.stringifyException(e));
      return -1;
    }
    
  }
}
