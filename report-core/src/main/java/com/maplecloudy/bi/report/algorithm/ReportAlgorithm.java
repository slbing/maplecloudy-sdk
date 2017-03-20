package com.maplecloudy.bi.report.algorithm;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;
import com.maplecloudy.avro.io.UnionData;
import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.ReportFrequence;
import com.maplecloudy.bi.model.report.ARKeys;
import com.maplecloudy.bi.model.report.ActivelyReportKey;
import com.maplecloudy.bi.model.report.RKey;
import com.maplecloudy.bi.model.report.ReportKey;
import com.maplecloudy.bi.model.report.ReportPair;

@SuppressWarnings({"rawtypes"})
public abstract class ReportAlgorithm<RK extends ReportKey,K,V> implements
    Configurable {
  public abstract List<ReportPair<RK>> algorithm(final K key, final V value)
      throws Exception;
  
  public ReportFrequence frequence = ReportFrequence.Hourly;
  public int reportTime = (int) System.currentTimeMillis() / 1000;
  public Configuration conf;
  
  public void init(Configuration conf) {
    frequence = ReportFrequence.valueOf(conf.get(
        ReportConstants.REPORT_FREQUENCE, ReportFrequence.Hourly.name()));
    reportTime = conf.getInt(ReportConstants.REPORT_TIME,
        (int) (System.currentTimeMillis() / 1000));
  }
  
  public void setConf(Configuration conf) {
    this.conf = conf;
    init(conf);
  }
  
  /** Return the configuration used by this object. */
  public Configuration getConf() {
    return this.conf;
  }
  
  public static void setReportAlgorithms(Configuration conf, String[] algorithms)
      throws Exception {
    conf.setStrings(ReportConstants.REPORT_ALGORITHMS, algorithms);
    List<Class> ts = Lists.newArrayList();
    for (String algorithm : algorithms) {
      ARKeys arks = Class.forName(algorithm).getAnnotation(ARKeys.class);
      if (null != arks) {
        for (Class cl : arks.activelyReportKey()) {
          ts.add(cl);
        }
      }
      RKey rk = Class.forName(algorithm).getAnnotation(RKey.class);
      ts.add(rk.reportKey());
    }
    
    UnionData.setUnionClass(conf, ts);
  }
  
  public static void setReportAlgorithms(Configuration conf,
      List<Class<? extends ReportAlgorithm>> ras) throws Exception {
    String[] algorithms = new String[ras.size()];
    int i = 0;
    for (Class<?> ra : ras) {
      algorithms[i] = ra.getName();
      i++;
    }
    setReportAlgorithms(conf, algorithms);
  }
  
  public static Class<? extends ActivelyReportKey>[] getActivelyReportKeys(
      Class<? extends ReportAlgorithm> ral) {
    if (ral.getAnnotation(ARKeys.class) == null) return null;
    return ral.getAnnotation(ARKeys.class).activelyReportKey();
  }
  
  public static Class<? extends ReportKey> getReportKey(
      Class<? extends ReportAlgorithm> ral) {
    return ral.getAnnotation(RKey.class).reportKey();
  }
  
  @SuppressWarnings("unchecked")
  public static Class<? extends ReportAlgorithm> getAlgorithm(
      Configuration conf, Class<? extends ReportKey> rk) {
    if (rk == null) throw new RuntimeException(
        "Can't get ReportAlgorithm from a null key!");
    Class[] ras = conf.getClasses(ReportConstants.REPORT_ALGORITHMS,
        new Class[0]);
    
    for (Class ra : ras) {
      if (!ReportAlgorithm.class.isAssignableFrom(ra)) {
        throw new RuntimeException(ra + " does not implement "
            + ReportAlgorithm.class.getName());
      }
      if (rk == getReportKey(ra)) return ra;
      
      Class<? extends ReportKey>[] keys = getActivelyReportKeys(ra);
      for (Class<?> key : keys) {
        if (rk == key) {
          return ra;
        }
      }
    }
    throw new RuntimeException(rk
        + " can't been found in the Configuration ReportAlgorithm:"
        + StringUtils.join(ras));
  }
  
  @SuppressWarnings("unchecked")
  public static boolean haveActivelyKey(Configuration conf) {
    Class[] ras = conf.getClasses(ReportConstants.REPORT_ALGORITHMS,
        new Class[0]);
    for (Class ra : ras) {
      if (getActivelyReportKeys(ra) != null
          && getActivelyReportKeys(ra).length > 0) return true;
    }
    return false;
  }
  
  public static Date getReportTime(Configuration conf) {
    return new Date((long) conf.getInt(ReportConstants.REPORT_TIME,
        (int) (System.currentTimeMillis() / 1000)) * 1000);
  }
  
  public static ReportFrequence getReportFrequence(Configuration conf) {
    return ReportFrequence.valueOf(conf.get(ReportConstants.REPORT_FREQUENCE,
        ReportFrequence.Hourly.name()));
  }
  
  public String getTableName() {
    String tblName = this.getClass().getSimpleName();
    if (ReportFrequence.NONE != getReportFrequence(conf)) {
      tblName = tblName
          + conf.get(ReportConstants.REPORT_FREQUENCE,
              ReportFrequence.Hourly.name());
    }
    return tblName;
  }
}
