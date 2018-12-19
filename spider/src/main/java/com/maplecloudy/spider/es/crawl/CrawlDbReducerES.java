package com.maplecloudy.spider.es.crawl;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import com.maplecloudy.avro.util.AvroUtils;
import com.maplecloudy.spider.crawl.CrawlDatum;
import com.maplecloudy.spider.metadata.Spider;

/** Merge new page entries with existing entries. */
public class CrawlDbReducerES
    extends Reducer<String,CrawlDatum,String,CrawlDatum> {
  public static final Log LOG = LogFactory.getLog(CrawlDbReducerES.class);
  
  private int retryMax;
  private CrawlDatum result = new CrawlDatum();
  
  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    Configuration job = context.getConfiguration();
    retryMax = job.getInt("db.fetch.retry.max", 3);
  }
  
  // private ArrayList<CrawlDatum> linked = new ArrayList<CrawlDatum>();
  
  @Override
  protected void reduce(String key, Iterable<CrawlDatum> values,
      Context context) throws IOException, InterruptedException {
    CrawlDatum fetch = null;
    CrawlDatum old = null;
    CrawlDatum link = null;
    result.clean();
    while (values.iterator().hasNext()) {
      CrawlDatum datum = values.iterator().next();
      // LOG.info("status:" + datum.getStatus() + " " + key);
      if (CrawlDatum.hasDbStatus(datum)) {
        if (old == null) {
          old = AvroUtils.clone(datum);
        } else {
          // always take the latest version
          if (old.getFetchTime() < datum.getFetchTime()) old.set(datum);
        }
        continue;
      }
      
      if (CrawlDatum.hasFetchStatus(datum)) {
        if (fetch == null) {
          fetch = AvroUtils.clone(datum);
        } else {
          if (fetch.getFetchTime() < datum.getFetchTime()) fetch.set(datum);
        }
        continue;
      }
      link = new CrawlDatum(datum);
    }
    
    // still no new data - record only unchanged old data, if exists, and
    // return
    if (fetch == null) {
      if (old != null) // at this point at least "old" should be
      {
        context.write(key, old);
        return;
      }
    }
    if (fetch == null && link != null) {
      fetch = link;
    }
    // initialize with the latest version, be it fetch or link
    result.set(fetch);
    if (old != null) {
      // copy metadata from old, if exists
      if (old.getMetaData().size() > 0) {
        result.getMetaData().putAll(old.getMetaData());
        // overlay with new, if any
        if (fetch.getMetaData().size() > 0)
          result.getMetaData().putAll(fetch.getMetaData());
      }
      // set the most recent valid value of modifiedTime
      if (old.getModifiedTime() > 0 && fetch.getModifiedTime() == 0) {
        result.setModifiedTime(old.getModifiedTime());
      }
      result.setRetriesSinceFetch(old.getRetriesSinceFetch());
    }
    
    switch (fetch.getStatus()) {
      case CrawlDatum.STATUS_FETCH_SUCCESS:
        
        result.setStatus(CrawlDatum.STATUS_DB_FETCHED);
        // result.setNextFetchTime();
        break;
      
      case CrawlDatum.STATUS_FETCH_REDIR_TEMP:
        result.setStatus(CrawlDatum.STATUS_DB_REDIR_TEMP);
        // result.setNextFetchTime();
        break;
      case CrawlDatum.STATUS_FETCH_REDIR_PERM:
        result.setStatus(CrawlDatum.STATUS_DB_REDIR_PERM);
        // result.setNextFetchTime();
        break;
      case CrawlDatum.STATUS_FETCH_RETRY:
        result.setRetriesSinceFetch(fetch.getRetriesSinceFetch() + 1);
        if (result.getRetriesSinceFetch() < retryMax) {
          result.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
        } else {
          result.setStatus(CrawlDatum.STATUS_DB_GONE);
        }
        break;
      case CrawlDatum.STATUS_LINKED:
        if (old != null) { // if old exists
          result.set(old); // use it
        } else {
          result.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
        }
        break;
      case CrawlDatum.STATUS_FETCH_GONE:
        result.setStatus(CrawlDatum.STATUS_DB_GONE);
        break;
      
      default:
        throw new RuntimeException(
            "Unknown status: " + fetch.getStatus() + " " + key);
    }
    
    result.getMetaData().remove(Spider.GENERATE_TIME_KEY);
    context.write(key, result);
  }
  
}