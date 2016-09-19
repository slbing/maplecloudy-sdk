package com.maplecloudy.bi.scheduler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.google.common.collect.Lists;
import com.maplecloudy.avro.io.UnionData;
import com.maplecloudy.avro.mapreduce.AvroJob;
import com.maplecloudy.avro.mapreduce.input.AvroPairInputFormat;
import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.ReportJob;
import com.maplecloudy.bi.mapreduce.GenericReport;
import com.maplecloudy.bi.mapreduce.output.ReportOutputFormat;
import com.maplecloudy.bi.model.report.ReportValues;
import com.maplecloudy.bi.report.algorithm.ReportAlgorithm;
import com.maplecloudy.bi.util.ConfigProperties;
import com.maplecloudy.bi.util.ReportConfiguration;
import com.maplecloudy.bi.util.ReportConstantsUtils;
import com.maplecloudy.oozie.main.OozieMain;
import com.maplecloudy.oozie.main.OozieToolRunner;
import com.maplecloudy.source.report.IntermediateSource;

@SuppressWarnings({"rawtypes"})
public class SchedulerReportJob extends OozieMain implements Tool,
    ReportConstants {
  
  public static final Log LOG = LogFactory.getLog(SchedulerReportJob.class);
  
  public Path processReport(Iterable<Path> inputs, Path output)
      throws Exception {
    AvroJob job = ReportJob.get(getConf());
    int reportTime = getConf().getInt(REPORT_TIME, 0);
    String jobInfo = getConf().get(REPORT_APP_NAME, this.getClass().getSimpleName()) + "-" + FORMAT_JOB_DATE.format(new Date(1000L*reportTime));
    job.setJobName(getConf().get(REPORT_APP_NAME, this.getClass().getSimpleName()) + ": process");
    job.setInputFormatClass(AvroPairInputFormat.class);
    
    FileSystem fs = FileSystem.get(getConf());
    for (Path input : inputs) {
      if (fs.exists(input)) {
        FileInputFormat.addInputPath(job, input);
        LOG.info("Add new input path: " + input);
      } else LOG.warn("Skip input path:" + input
          + ", for it's does not exists!");
    }
    job.setMapperClass(GenericReport.M.class);
    job.setCombinerClass(GenericReport.R.class);
    job.setReducerClass(GenericReport.R.class);
    Path tmpOutput = ReportConstantsUtils.getTmpSegmentDir(output.toString(), jobInfo);
    LOG.info("Outpath path: " + output);


    FileOutputFormat.setOutputPath(job, tmpOutput);
    job.setMapOutputKeyClass(UnionData.class);
    job.setMapOutputValueClass(ReportValues.class);
    job.setOutputKeyClass(UnionData.class);
    job.setOutputValueClass(ReportValues.class);
    job.setOutputFormatClass(ReportOutputFormat.class);
    
    if (runJob(job)) {
      Path ioutput = IntermediateSource.get().getOutput(
          ReportAlgorithm.getReportTime(getConf()),
          ReportAlgorithm.getReportFrequence(getConf()));
      ReportConstantsUtils.installChild(ioutput, tmpOutput, getConf());
      return output;
    } else {
      throw new Exception("job faild, please look in for detail.");
    }
  }
  
  public Path processActivelyKey(Path output) throws Exception {
    // if (!ReportAlgorithm.haveActivelyKey(getConf()))
    // {
    // Path p = IntermediateSource.get().getIntermediateOutput(
    // ReportAlgorithm.getReportTime(getConf()),ReportAlgorithm.getReportFrequence(getConf()));
    //
    // return output;
    // }
    // Configuration conf = ReportConfiguration.create("report-m-two.xml");
    // start the phase2 job
    Class[] ras = getConf().getClasses(ReportConstants.REPORT_ALGORITHMS, new Class[0]);
    AvroJob job = ReportJob.get(getConf());
    job.getConfiguration().setBoolean("report.process.actively", true);
    int reportTime = getConf().getInt(REPORT_TIME, 0);
    String jobInfo = getConf().get(REPORT_APP_NAME, this.getClass().getSimpleName()) + "-" + FORMAT_JOB_DATE.format(new Date(1000L*reportTime));
    job.setJobName(getConf().get(REPORT_APP_NAME, this.getClass().getSimpleName()) + ": processActivelyKey");
    job.setInputFormatClass(AvroPairInputFormat.class);
    Iterable<Path> inputs = IntermediateSource.get().getInputs(
        ReportAlgorithm.getReportTime(getConf()),
        ReportAlgorithm.getReportFrequence(getConf()), ras);
    for (Path input : inputs) {
      FileInputFormat.addInputPath(job, input);
    }
    job.setMapperClass(GenericReport.M2.class);
    // modify by dxzhang3
    job.setCombinerClass(GenericReport.R2.class);
    job.setReducerClass(GenericReport.R2.class);
    
    Path tmpOutput = ReportConstantsUtils.getTmpSegmentDir(output.toString(), jobInfo);
    FileOutputFormat.setOutputPath(job, tmpOutput);
    job.setMapOutputKeyClass(UnionData.class);
    job.setMapOutputValueClass(ReportValues.class);
    job.setOutputKeyClass(UnionData.class);
    job.setOutputValueClass(ReportValues.class);
    job.setOutputFormatClass(ReportOutputFormat.class);
    if (runJob(job)) {
      ReportConstantsUtils.installChild(output, tmpOutput, getConf());
      return tmpOutput;
    } else {
      throw new Exception("job faild, please look in for detail.");
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args == null || args.length < 1) {
      System.out.println("Usage: <conf-file>");
      return -1;
    }
    String confFile = args[0];
    ConfigProperties commConf = new ConfigProperties("/" + confFile);
    List<String> inputs = Arrays.asList(org.apache.hadoop.util.StringUtils
        .getTrimmedStrings(commConf.getString("data.inputs")));
    Path output = new Path(commConf.getString("data.output"));
    String[] algorithms = org.apache.hadoop.util.StringUtils
        .getTrimmedStrings(commConf
            .getString(ReportConstants.REPORT_ALGORITHMS));
    // int numReduce = commConf.getInt("reduce.num", 1);
    // String bloom = commConf.getString("bloom.uuid", null);
    List<Path> inputPaths = Lists.newArrayList();
    for (String input : inputs) {
      inputPaths.add(new Path(input));
    }
    ReportAlgorithm.setReportAlgorithms(getConf(), algorithms);
    // getConf().setStrings(ReportConstants.REPORT_ALGORITHMS, algorithms);
    getConf().setInt(
        ReportConstants.REPORT_TIME,
        (int) (ReportConstants.FORMAT_OOZIE.parse(
            commConf.getString("start.time")).getTime() / 1000));
    processReport(inputPaths, output);
    processActivelyKey(output);
    
    return 0;
  }
  
  // final static String oozieFork =
  // ReportConstantsUtils.loadFile("/oozie-fork.xml");
  // final static String sqoopAction =
  // ReportConstantsUtils.loadFile("/oozie-sqoop.xml");
  
  // public String createSqoopFlow(Path output) throws IOException {
  // List<ReportAlgorithm> ras = getConf().getInstances(
  // ReportConstants.REPORT_ALGORITHMS, ReportAlgorithm.class);
  // ReportFrequence frequence = ReportFrequence.valueOf(getConf().get(
  // ReportConstants.REPORT_FREQUENCE, ReportFrequence.Hourly.name()));
  // int reportTime = getConf().getInt(ReportConstants.REPORT_START_TIME,
  // (int) (System.currentTimeMillis() / 1000));
  // String exportFlow = oozieFork;
  // for (ReportAlgorithm ral : ras) {
  // ral.init(getConf());
  // if (!DbOutputAble.class.isAssignableFrom(ral.getClass())) continue;
  // DbOutputAble ra = (DbOutputAble) ral;
  // if (null == ra.getTableName() || StringUtils.isBlank(ra.getTableName()))
  // continue;
  // Path report = new Path(ConstantsUtils.getCurrentDir(output), ral
  // .getCollectKeysClass().getSimpleName());
  // // if (report.getFileSystem(getConf()).exists(report)) {
  // exportFlow = exportFlow.replace("${replaceFork}",
  // "<path start=\"export-" + ra.getTableName() + "\"/>\n${replaceFork}")
  // .replace("${replaceAction}",
  // getAction(ra, report) + "\n${replaceAction}");
  // // }
  // }
  // if (exportFlow != null) exportFlow = exportFlow.replace("${replaceFork}",
  // "").replace("${replaceAction}", "");
  // Path sqoopFlow = ConstantsUtils.getSubflowOutput(
  // new Date(reportTime * 1000), frequence);
  //
  // Path tmpFlow = ConstantsUtils.getTmpSegmentDir(sqoopFlow);
  //
  // saveSubFlow(tmpFlow, exportFlow);
  // ConstantsUtils.install(sqoopFlow, tmpFlow);
  // super.putActionData("subFlow",
  // new Path(ConstantsUtils.getCurrentDir(sqoopFlow), "workflow")
  // .toString());
  // super.storeData();
  // return exportFlow;
  // }
  
  public void saveSubFlow(Path output, String subFlow) throws IOException {
    FileSystem fs = output.getFileSystem(getConf());
    FSDataOutputStream out = fs.create(
        new Path(output, "workflow/workflow.xml"), true);
    out.write(subFlow.getBytes("UTF-8"));
    out.flush();
    out.close();
  }
  
  // public String getAction(DbOutputAble ra, Path report) {
  // String action = sqoopAction
  // .replace("${dbUrl}", ReportProperties.dbConf.getProperty("jdbc.url"))
  // .replace("${dbUser}",
  // ReportProperties.dbConf.getProperty("jdbc.username"))
  // .replace("${dbPassword}",
  // ReportProperties.dbConf.getProperty("jdbc.password"))
  // .replace("${dbTable}", ra.getTableName())
  // .replace("${columns}", ra.getKeys() + "," + ra.getValues())
  // .replace("${exportDir}", report.toString())
  // .replace("${reportName}", ra.getTableName());
  // return action;
  // // ${dbTable} --export-dir ${exportDir} --columns ${columns}
  // }
  
  public static void main(String[] args) throws Exception {
    int res = OozieToolRunner.run(ReportConfiguration.create(),
        new SchedulerReportJob(), args);
    System.exit(res);
  }
}