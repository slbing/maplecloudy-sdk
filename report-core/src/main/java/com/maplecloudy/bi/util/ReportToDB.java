package com.maplecloudy.bi.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;

import com.google.common.collect.Lists;
import com.maplecloudy.avro.io.MapAvroFile;
import com.maplecloudy.avro.io.Pair;
import com.maplecloudy.avro.io.UnionData;
import com.maplecloudy.avro.mapreduce.output.AvroMapOutputFormat;
import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.ReportFrequence;
import com.maplecloudy.bi.mapreduce.output.db.DBConfiguration;
import com.maplecloudy.bi.mapreduce.output.db.ReportDbOutputFormat;
import com.maplecloudy.bi.model.report.ReportKey;
import com.maplecloudy.bi.model.report.ReportValues;
import com.maplecloudy.bi.report.algorithm.ReportAlgorithm;
import com.maplecloudy.oozie.main.OozieMain;
import com.maplecloudy.oozie.main.OozieToolRunner;
import com.maplecloudy.source.report.ReportSource;

@SuppressWarnings({ "rawtypes" })
public class ReportToDB extends OozieMain implements Tool, ReportConstants {
    /*
     * 本模块有两种调用方式: 
     * 1 当作为借口被报表算法调用时, conf集成继承与框架的configuration,
     *   这时候报表算法入口是在bi-report中. conf在创建时会加载report.xml配置文件读取数据库连接信息
     *  2 当作为单独进程启动时, 此时需要在oozie-action中设置数据库连接信息 需要下面几个参数:
     * -mapreduce.jdbc.driver.class 
     * -mapreduce.jdbc.url 
     * -mapreduce.jdbc.username
     * -mapreduce.jdbc.password
     */
    Configuration conf = null;
    DBConfiguration dbConf = null;
    Connection conn = null;
    String userName = null;
    static ReportToDB toDB = null;

    public ReportToDB() throws ClassNotFoundException, SQLException {
        conf = ReportConfiguration.create();
        dbConf = new DBConfiguration(conf);
        conn = dbConf.getConnection();
        conn.setAutoCommit(false);
    }

    /* 多线程不可入接口, 如果多线程环境请创建多个ReportToDB对象 */
    public static ReportToDB get() throws ClassNotFoundException, SQLException {
        if (null == toDB)
            toDB = new ReportToDB();

        return toDB;
    }
    
    public String getTableName(Class<? extends ReportAlgorithm> ra, ReportFrequence freq)
    {
        if(freq == ReportFrequence.NONE)
        {
            return ra.getSimpleName();
        }
        else
        {
            return ra.getSimpleName() + freq.name();
        }
    }

    void deleteRecord(Date startDate, ReportFrequence freq, List<Class<? extends ReportAlgorithm>> ras) throws SQLException {
        int startTime = (int) (startDate.getTime() / 1000);
        for (Class<? extends ReportAlgorithm> ra : ras) {
            Statement delSt = conn.createStatement();
            delSt.setQueryTimeout(60);
            ReportAlgorithm raObject = ReflectionUtils.newInstance(ra, conf);
            String tableName = raObject.getTableName();
            String sqlDel = "delete from " + tableName + " where timestamp=" + startTime;

            if (true != delSt.execute(sqlDel)) {
                System.out.println("deleteRecord success: (" + sqlDel + "), startDate=" + startDate.toString());
            } else {
                System.out.println("deleteRecord fail: (" + sqlDel + "), startDate=" + startDate.toString());
            }
            delSt.close();
        }

        /*
         * 执行事务之后, 应该尽早提交, 否则会出现mysql的死锁、或锁等待超时现象, 如下: java.sql.SQLException:
         * Lock wait timeout exceeded; try restarting transactio
         */
        conn.commit();

    }

    void deleteRecord(Date startDate, ReportFrequence freq, String[] algorithms) throws ClassNotFoundException, SQLException {
        List<Class<? extends ReportAlgorithm>> ras = ReportUtils.getAlgorithmClasses(algorithms);
        deleteRecord(startDate, freq, ras);
    }

    List<Class<? extends ReportAlgorithm>> updateAlgorithms(Date startDate, ReportFrequence freq, List<Class<? extends ReportAlgorithm>> ras) throws ClassNotFoundException, IOException, SQLException {
        List<Class<? extends ReportAlgorithm>> lstRas = Lists.newArrayList();
        int startTime = (int) (startDate.getTime() / 1000);
        for (Class<? extends ReportAlgorithm> ra : ras) {
            ReportAlgorithm raObject = ReflectionUtils.newInstance(ra, conf);
            String tableName = raObject.getTableName();

            Statement selectSt = conn.createStatement();
            selectSt.setQueryTimeout(60);
            String sqlselect = "select count(*) from " + tableName + " where timestamp=" + startTime;
            ResultSet ret = selectSt.executeQuery(sqlselect);
            if (null != ret && ret.next()) {
                String count = ret.getString(1);
                if (StringUtils.isNumeric(count)) {
                    if (0 == count.compareTo("0")) {
                        /* 表中当前没有该事件戳的记录, 则需要将algorithm入库 */
                        lstRas.add(ra);
                    }
                    System.out.println("updateAlgorithms success: (" + sqlselect + ") return count=" + count + ", startDate=" + startDate.toString());
                } else {
                    /* 返回值异常不入库, 打印日志, 后面人工干预 */
                    System.out.println("updateAlgorithms fail: (" + sqlselect + ") return " + count + ", startDate=" + startDate.toString());
                }
            } else {
                /* 返回值异常不入库, 打印日志, 后面人工干预 */
                System.out.println("updateAlgorithms fail: (" + sqlselect + ") return null, startDate=" + startDate.toString());
            }
            selectSt.close();
        }

        conn.commit();

        /* 当lstAlgs长度为0时, 返回一个元素个数为0的有效数组 */
        return lstRas;
    }

    List<String> updateAlgorithms(Date startDate, ReportFrequence freq, String[] algorithms) throws ClassNotFoundException, IOException, SQLException {
        List<Class<? extends ReportAlgorithm>> rasTmp = ReportUtils.getAlgorithmClasses(algorithms);
        List<Class<? extends ReportAlgorithm>> ras = updateAlgorithms(startDate, freq, rasTmp);
        return ReportUtils.getAlgorithmString(ras);
    }

    public int report2Db(Date startDate, ReportFrequence freq, boolean forceupdate, List<Class<? extends ReportAlgorithm>> ras) throws Exception {
        /* 设置时间属性 */
        conf.setInt(REPORT_TIME, (int) (startDate.getTime() / 1000));
        conf.set(REPORT_FREQUENCE, freq.name());
        
        List<Class<? extends ReportAlgorithm>> useRas = ras;
        if (forceupdate) {
            /* 强制更新时, 数据表中若存在该startDate的数据则全部删除 */
            deleteRecord(startDate, freq, ras);
        } else {
            /* 不强制更新时, 数据表中若存在该startDate时则不用再次入库, 从algorithms中将不用入库的算法去除 */
            useRas = updateAlgorithms(startDate, freq, ras);
        }

        if (useRas.size() <= 0) {
            System.out.println("runReport: all algorithms has exported to DB, not need to export again!");
            return 0;
        }

        ReportAlgorithm.setReportAlgorithms(conf, useRas);
        List<Path> tmpInputs = ReportSource.get().getInputs(startDate, freq, ras, userName);
        List<Path> inputs = Lists.newArrayList();
        FileSystem fs = FileSystem.get(conf);
        for (Path p : tmpInputs) {
            Path currPath = ReportConstantsUtils.getCurrentDir(p);
            if(fs.exists(currPath) && fs.isDirectory(currPath))
            {
                inputs.add(currPath);
            }
            else
            {
                System.out.println("runReport: (" + currPath.toString() + ") not exist or Dir, cannot be added to inputs!");
            }
        }
        
        if(inputs.size() <= 0)
        {
            System.out.println("runReport: ReportSource.get().getInputs return empty");
            return -1;
        }

        Job job = new Job(conf);
        job.setOutputFormatClass(ReportDbOutputFormat.class);
        job.setOutputKeyClass(ReportKey.class);
        job.setOutputValueClass(ReportValues.class);
        TaskAttemptContext taskContext = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
        ReportDbOutputFormat rdf = new ReportDbOutputFormat();
        RecordWriter<ReportKey, ReportValues> writer = rdf.getRecordWriter(taskContext);
        for (Path input : inputs) {
            long lAlgRecordCount = 0;
            MapAvroFile.Reader<Object, ReportValues>[] reads = AvroMapOutputFormat.getReaders(input, conf);
            for (MapAvroFile.Reader<Object, ReportValues> read : reads) {
                while (read.hasNext()) {
                    Pair<Object, ReportValues> pair = read.next();
                    ReportKey rk = pair.key() instanceof UnionData ? (ReportKey) ((UnionData) pair.key()).datum : (ReportKey) pair.key();
                    writer.write(rk, pair.value());
                    lAlgRecordCount++;
                }
            }
            System.out.println("runReport: write " + lAlgRecordCount + " records to DB from " + input.toString());
        }

        /*
         * AsyncSqlRecordWriter.close原来在mapreduce框架里面调用, 这里要显示调用: 1
         * sql语句每100条批处理一次, close里面完成剩余的sql语句执行 2 close里面会阻塞等待sql执行线程结束才退出,
         * 如果不调用主线程会退出, 进程退出
         */
        writer.close(taskContext);
        return 0;
    }

    public int report2Db(Date startDate, ReportFrequence freq, boolean forceupdate, String[] algorithms) throws Exception {
        List<Class<? extends ReportAlgorithm>> ras = ReportUtils.getAlgorithmClasses(algorithms);
        return report2Db(startDate, freq, forceupdate, ras);
    }

    public int runLoop(Date startDate, ReportFrequence freq, boolean forceupdate, String[] algorithms) throws Exception {
        int iRet = 0;
        if (ReportFrequence.Hourly == freq || ReportFrequence.NONE == freq) {
            iRet = report2Db(startDate, ReportFrequence.Hourly, forceupdate, algorithms);
        } else if (ReportFrequence.Daily == freq) {
            Date endDate = ReportConstantsUtils.getEdndate(startDate, ReportFrequence.Daily);
            Calendar start = Calendar.getInstance();
            Calendar end = Calendar.getInstance();
            end.setTime(endDate);
            start.setTime(startDate);
            while (start.before(end)) {
                iRet = report2Db(start.getTime(), ReportFrequence.Hourly, forceupdate, algorithms);
                if(0 != iRet)
                {
                    return iRet;
                }
                
                start.add(Calendar.HOUR, 1);
            }

            iRet = report2Db(startDate, ReportFrequence.Daily, forceupdate, algorithms);
        } else if (ReportFrequence.Monthly == freq || ReportFrequence.Weekly == freq||ReportFrequence.Thirtydays == freq) {
            Date endDate = ReportConstantsUtils.getEdndate(startDate, freq);
            Calendar start = Calendar.getInstance();
            Calendar end = Calendar.getInstance();
            end.setTime(endDate);
            start.setTime(startDate);
            while (start.before(end)) {
                iRet = runLoop(start.getTime(), ReportFrequence.Daily, forceupdate, algorithms);
                if(0 != iRet)
                {
                    return iRet;
                }
                
                start.add(Calendar.DATE, 1);
            }

            iRet = report2Db(startDate, freq, forceupdate, algorithms);
        }
        
        return iRet;
    }

    /*
     * Description: 入库入口函数
     * @param starttime: 起始日期
     * @param frequence: 日期频度, 如果此频度比最小频度(Hourly)大, 内部用反复调用小粒度实现
     * @param forceupdate: 默认为false 
     *        true-当前时间粒度的记录存在的话全部删除, 重新生成;
     *        false-当前时间粒度的记录存在的话全部删除, 不用在入库;
     * @param algorithms: 需要入库的报表类型
     * @param isLoop: 是否需要递归入库(即入库月报表数据, 把Daily、Hourly全部入库), 默认false
     * @return int: 0-成功; 否则失败
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        if (args == null || args.length < 4) {
            System.out.println("Usage: <starttime> <frequence such as: " + StringUtils.join(ReportFrequence.values(), ",") + 
                    "> <forceupdate> <algorithms> [isLoop] [-username u]");
            return -1;
        }

        Date startDate = FORMAT_OOZIE.parse(args[0]);
        ReportFrequence freq = ReportFrequence.valueOf(args[1]);
        String[] algorithms = org.apache.hadoop.util.StringUtils.getTrimmedStrings(args[3]);
        boolean forceupdate = Boolean.valueOf(args[2]);

        boolean isLoop = false;
        if (args.length > 4)
            isLoop = Boolean.valueOf(args[4]);

        if (args.length > 4) {
            for (int idx = 4; idx < args.length; idx++) {
              if ("-username".equalsIgnoreCase(args[idx])) {
                if (++idx == args.length) {
                  throw new IllegalArgumentException("user name not specified in -username");
                }
                userName = args[idx];
              }
            }
          }
        
        int iRet = 0;
        if (isLoop) {
            iRet = runLoop(startDate, freq, forceupdate, algorithms);
        } else {
            iRet = report2Db(startDate, freq, forceupdate, algorithms);
        }
        return iRet;
    }

    public static void main(String[] args) throws Exception {
        int ret = OozieToolRunner.run(ReportConfiguration.create(), new ReportToDB(), args);
        System.exit(ret);
    }
}
