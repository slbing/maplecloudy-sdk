package com.maplecloudy.bi.mapreduce;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.maplecloudy.avro.io.UnionData;
import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.ReportFrequence;
import com.maplecloudy.bi.interceptor.TimestampInterceptor;
import com.maplecloudy.bi.model.report.ActivelyReportKey;
import com.maplecloudy.bi.model.report.ReportKey;
import com.maplecloudy.bi.model.report.ReportPair;
import com.maplecloudy.bi.model.report.ReportValues;
import com.maplecloudy.bi.report.algorithm.ReportAlgorithm;
import com.maplecloudy.bi.util.FilterUtil;
import com.maplecloudy.bi.util.ReportConstantsUtils;
import com.maplecloudy.bi.util.ReportUtils;

/**
 * @author kim
 * @date 2012-11-11
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GenericReport {
  public static class M<K,V> extends Mapper<K,V,UnionData,ReportValues> {
    List<ReportAlgorithm> ras = null;
    TimestampInterceptor ti = null;
    public ReportFrequence frequence = ReportFrequence.Hourly;
    public long reportTime = System.currentTimeMillis();
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      ras = context.getConfiguration().getInstances(
          ReportConstants.REPORT_ALGORITHMS, ReportAlgorithm.class);
      frequence = ReportFrequence.valueOf(context.getConfiguration().get(
          ReportConstants.REPORT_FREQUENCE, ReportFrequence.Hourly.name()));
      reportTime = context.getConfiguration().getInt(
          ReportConstants.REPORT_TIME,
          (int) (System.currentTimeMillis() / 1000));
      ti = new TimestampInterceptor(reportTime, ReportConstantsUtils.getEdndate(
          new Date(reportTime * 1000), frequence).getTime());
      if (ras == null || ras.size() < 1) throw new IOException(
          "No algorithms been set!");
      for (ReportAlgorithm ra : ras)
        ra.init(context.getConfiguration());
      //add some info for debug
      FileSplit fp = ((FileSplit)context.getInputSplit());
      System.out.println("start:"+fp.getStart()+" length:"+fp.getLength()); 
      
      FilterUtil.set(context.getConfiguration());
      FilterUtil.get().setFilter();
    }
    int pnum = 0;
    @Override
    protected void map(K key, V value, Context context) throws IOException,
        InterruptedException {
      ++pnum;
      if(pnum%10000 == 0)
        System.out.println("current process: " + pnum);
      
      if (key instanceof UnionData)// other reuse ReportKey for up daily
      // report
      {
        context.write((UnionData) key, (ReportValues) value);
      } else {
        pressLog(key, value, context);
      }
    }
    
    protected void pressLog(K k, V v, Context context) {
      
      // log = ti.intercept(log);
      // if (log == null) {
      // context.getCounter("Mapper-Intercept", "Skip-Num").increment(1);
      // return;
      // }
      //
      try {
        for (ReportAlgorithm ra : ras) {
          try {
            List<ReportPair> lst = ra.algorithm(k, v);
            if (lst == null) continue;
            for (ReportPair pair : lst) {
              context.getCounter("Mapper-ReportAlgorithm-Outputkeys",
                  pair.key.getClass().getSimpleName()).increment(1);
              context.write(new UnionData(pair.key), pair.reportValues);
            }
          } catch (Exception e) {
            context.getCounter("Mapper-Error", ra.getClass().getSimpleName())
                .increment(1);
            e.printStackTrace();
          }
        }
        
      } catch (Exception e) {
        context.getCounter("Mapper-Error", "Unexpected").increment(1);
        e.printStackTrace();
      }
    }
    
    
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      System.out.println("current process:"+pnum);
    }
  }
  
  public static class R extends
      Reducer<UnionData,ReportValues,UnionData,ReportValues> {
    
    @Override
    protected void reduce(UnionData key, Iterable<ReportValues> values,
        Context context) throws IOException, InterruptedException {
      context.getCounter("reducer-key", key.datum.getClass().getSimpleName()).increment(1);
      ReportValues reportValues = new ReportValues();
      for (ReportValues rv : values) {
        reportValues.merge(rv);
      }
      context.write(key, reportValues);
    }
  }
  
  public static class M2 extends
      Mapper<UnionData,ReportValues,UnionData,ReportValues> {
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {

    }
    
    @Override
    protected void map(UnionData key, ReportValues value, Context context)
        throws IOException, InterruptedException {
      if (value == null) return;
      try {
        if (key.datum instanceof ActivelyReportKey) {
          ActivelyReportKey ark = (ActivelyReportKey) key.datum;
          ReportKey rk  = ark.getSuperKey();
          Map<String,List<Integer>> map = ReportUtils.getActivelys(rk.getClass(),context.getConfiguration());
          ReportValues rv = new ReportValues();
          for (Entry<String,List<Integer>> entry : map.entrySet()) {
            Long it = value.getMerge(entry.getKey());
            if (it != null) {
              List<Integer> lst = entry.getValue();
              for (int i : lst) {
                if (it > i) {
                  rv.putMerge(entry.getKey() + (i == 0 ? "" : "-" + i), 1L);
                  // context.write(key, );
                }
              }
            }
          }
          if (rv.size() > 0) {
            context.write(new UnionData(ark.getSuperKey()), rv);
          }
        } else {
          context.write(key, value);
        }
      } catch (Exception e) {
        // context.getCounter("Bad-Line", type.name()).increment(1);
        e.printStackTrace();
      }
    }
  }
  
  
  public static class R2 extends
      Reducer<UnionData,ReportValues,UnionData,ReportValues> {
    @Override
    protected void reduce(UnionData key, Iterable<ReportValues> values,
        Context context) throws IOException, InterruptedException {
      ReportValues reportValues = new ReportValues();
      for (ReportValues rv : values) {
        reportValues.merge(rv);
      }
      
      context.write(key, reportValues);
    }
  }
}
