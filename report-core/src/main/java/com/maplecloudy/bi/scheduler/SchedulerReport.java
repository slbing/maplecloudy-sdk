package com.maplecloudy.bi.scheduler;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import com.google.common.collect.Lists;
import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.ReportFrequence;
import com.maplecloudy.bi.report.algorithm.DbOutputAble;
import com.maplecloudy.bi.report.algorithm.ReportAlgorithm;
import com.maplecloudy.bi.util.ReportConfiguration;
import com.maplecloudy.bi.util.ReportConstantsUtils;
import com.maplecloudy.bi.util.ReportUtils;
import com.maplecloudy.bi.util.todb.ReportToDbAlg;
import com.maplecloudy.oozie.main.OozieMain;
import com.maplecloudy.oozie.main.OozieToolRunner;
import com.maplecloudy.source.report.IntermediateSource;
import com.maplecloudy.source.report.ReportSource;

@SuppressWarnings({"rawtypes"})
public class SchedulerReport extends OozieMain implements Tool, ReportConstants {
  String  userName = null;
  boolean bIsToDB = true;
  
  // Configuration pureConf = null;
  
  @Override
  public int run(String[] args) throws Exception {
    if (args == null || args.length < 4) {
      System.out.println("Usage: <starttime> <frequence such as: "
          + StringUtils.join(ReportFrequence.values(), ",")
          + "> <forceupdate> <algorithms> [-username u]");
      return -1;
    }
    
    if (args.length > 2) {
      for (int idx = 2; idx < args.length; idx++) {
        if ("-username".equalsIgnoreCase(args[idx])) {
          if (++idx == args.length) {
            throw new IllegalArgumentException(
                "user name not specified in -username");
          }
          userName = args[idx];
        }
        else if ("-is_todb".equalsIgnoreCase(args[idx])) {
          if (++idx == args.length) {
            throw new IllegalArgumentException(
                "user name not specified in -username");
          }
          bIsToDB = Boolean.valueOf(args[idx]);
        }else if ("-dbuserdef".equalsIgnoreCase(args[idx])) {
        	ReportToDbAlg.get(this.getConf()).setDbUserDef(true);
        }
      }
    }
    
    initSpecial();
    
    Date startDate = FORMAT_OOZIE.parse(args[0]);
    boolean forceupdate = false;
    ReportFrequence frequence = ReportFrequence.valueOf(args[1]);
    String[] algorithms = null;
    if (args.length > 3) algorithms = org.apache.hadoop.util.StringUtils.getTrimmedStrings(args[3]);
    if (args.length > 2) forceupdate = Boolean.valueOf(args[2]);
    ReportAlgorithm.setReportAlgorithms(getConf(), algorithms);
    getConf().setInt(REPORT_TIME, (int) (startDate.getTime() / 1000));
    getConf().set(REPORT_FREQUENCE, args[1]);
    getConf().set(REPORT_APP_NAME, this.getClass().getSimpleName());
    runReport(startDate, frequence, forceupdate);
    return 0;
  }
  
  public void initSpecial() throws Exception{
	  
  }
  
  public List<Path> getInput(Date startDate, ReportFrequence fq, String userName) {
    return null;
  }
  
  public Path getOutput(Date startDate, ReportFrequence fq) {
    return null;
  }
  
  public ReportFrequence getMinFrequence() {
    return ReportFrequence.Hourly;
  }
  
  public void runReport(Date startDate, ReportFrequence fq, boolean forceupdate)
      throws Exception {
    Date endDate = ReportConstantsUtils.getEdndate(startDate, fq);
    Iterable<Path> inputs;
    ReportFrequence minFreq = getMinFrequence();
    
    if (minFreq == fq || ReportFrequence.NONE == fq) {
      // inputs
      inputs = getInput(startDate, fq, userName);
    } else {
      Calendar start = Calendar.getInstance();
      Calendar end = Calendar.getInstance();
      end.setTime(endDate);
      start.setTime(startDate);
      Class[] ras = getConf().getClasses(ReportConstants.REPORT_ALGORITHMS,
          new Class[0]);
      
      if (ReportFrequence.Daily == fq) {
        // every hour of the day
        while (start.before(end)) {
          runReport(start.getTime(), ReportFrequence.Hourly, forceupdate);
          start.add(Calendar.HOUR, 1);
        }
        // inputs
        inputs = IntermediateSource.get().getInputs(startDate, endDate,
            ReportFrequence.Hourly, ras);
      } else if (ReportFrequence.Weekly == fq || ReportFrequence.Monthly == fq||ReportFrequence.Thirtydays == fq) {
        // every day in a month or a week
        while (start.before(end)) {
          runReport(start.getTime(), ReportFrequence.Daily, forceupdate);
          start.add(Calendar.DATE, 1);
        }
        // inputs
        inputs = IntermediateSource.get().getInputs(startDate, endDate,
            ReportFrequence.Daily, ras);
      } else {
        return;
      }
    }

    if (null == inputs || !inputs.iterator().hasNext()) {
      //throw new NullPointerException("input path is null.");
    	LOG.warn("input path is null, ReportAlgorithm exists!");
    	return;
    }
    Path output = getOutput(startDate, fq);
    if (null == output) {
      output = ReportSource.get().getOutput(startDate, fq);
    }
    List<ReportAlgorithm> algorithms = getConf().getInstances(
        ReportConstants.REPORT_ALGORITHMS, ReportAlgorithm.class);
    
    runReport(algorithms, inputs, output, startDate, fq, forceupdate);
  }
  
    /* 报表算法入库接口, 子类集成实现具体入库过程 */
    public void exportToDB(Date startDate, ReportFrequence freq, boolean forceupdate, String[] algorithms) throws SQLException, Exception {
      List<Class<? extends ReportAlgorithm>> ras = ReportUtils.getAlgorithmClasses(algorithms);
      List<Class<? extends ReportAlgorithm>> rets = Lists.newArrayList();
      for (Class<? extends ReportAlgorithm> ra : ras) {
          if (DbOutputAble.class.isAssignableFrom(ra)) {
              rets.add(ra);
          }
      }
      if (rets.size() > 0) {
    	  ReportToDbAlg todb = ReportToDbAlg.get(this.getConf());
    	  todb.setModelName("reportAlg");
    	  todb.report2Db(startDate, freq, forceupdate, rets);
      }
    }
  
  public void runReport(List<ReportAlgorithm> algorithms,
      Iterable<Path> inputs, Path output, Date inputDate, ReportFrequence fq,
      boolean forceupdate) throws Exception {
    Configuration conf = new Configuration(getConf());
    
    List<ReportAlgorithm> requireAlgorithms = Lists.newArrayList();
    if (!forceupdate) {
      // filter already exists ReportAlgorithm
      FileSystem fs = output.getFileSystem(conf);
      for (ReportAlgorithm algorithm : algorithms) {
        Path requireDir = ReportConstantsUtils.getCurrentDir(new Path(output,
            algorithm.getClass().getSimpleName()));
        if (!fs.exists(requireDir)) {
          requireAlgorithms.add(algorithm);
        } else {
          System.out.println("skip this ReportAlgorithm["
              + algorithm.getClass().getName() + "] (" + requireDir.toString()
              + ") have exists");
        }
      }
      if (null == requireAlgorithms || requireAlgorithms.isEmpty()) return;
    } else {
      requireAlgorithms = algorithms;
    }
    
    String[] ra = new String[requireAlgorithms.size()];
    for (int i = 0; i < requireAlgorithms.size(); i++) {
      ra[i] = requireAlgorithms.get(i).getClass().getName();
    }
    ReportAlgorithm.setReportAlgorithms(conf, ra);
    conf.setInt(REPORT_TIME, (int) (inputDate.getTime() / 1000));
    conf.set(REPORT_FREQUENCE, fq.name());
    
    FileSystem fs = FileSystem.get(conf);
    boolean hasInput = false;
    for (Path input : inputs) {
      if (fs.exists(input)) {
    	  hasInput = true;
    	  break;
      } else {
      	LOG.warn("input[" + input.toString() + "] not exist!");
      }
    }
    if (!hasInput){
    	LOG.warn("Have no avalible inputs, ReportAlgorithm exists!");
    	return;
    }
    
    SchedulerReportJob grm = new SchedulerReportJob();
    grm.setConf(conf);
    grm.processReport(inputs, output);
    grm.processActivelyKey(output);
    
    /*
     * 入库时, forceupdate总为true, 即总是把报表算法新生成的文件入库
     * 如果单独启动入库工具, 有选择性地入库, 可以置forceupdate为false
     */
    if(true == bIsToDB)
    {
    	exportToDB(inputDate, fq, true, ra);
    }
    else
    {
    	System.err.println("exportToDB: config not to DB");
    }
  }
  
  public static int jobRun(SchedulerReport r, String[] args) throws Exception {
    return OozieToolRunner.run(ReportConfiguration.create(), r, args);
  }
}
