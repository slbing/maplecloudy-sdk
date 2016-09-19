package com.maplecloudy.bi.util.todb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Tool;

import com.google.common.collect.Lists;
import com.maplecloudy.avro.io.MapAvroFile;
import com.maplecloudy.avro.io.Pair;
import com.maplecloudy.avro.io.UnionData;
import com.maplecloudy.avro.mapreduce.AvroJob;
import com.maplecloudy.avro.mapreduce.input.AvroPairInputFormat;
import com.maplecloudy.avro.mapreduce.output.AvroMapOutputFormat;
import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.ReportJob;
import com.maplecloudy.bi.mapreduce.output.db.DBConfiguration;
import com.maplecloudy.bi.util.ReportConfiguration;
import com.maplecloudy.bi.util.ReportConstantsUtils;
import com.maplecloudy.oozie.main.OozieMain;
import com.maplecloudy.oozie.main.OozieToolRunner;

public class ReportToDb extends OozieMain implements Tool, ReportConstants {
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
	//Configuration conf = null;
	DBConfiguration dbConf = null;
	public Connection cur_conn = null;
	protected String userName = null;
	//private String tableName;
	protected static ReportToDb toDB = null;
	public static final int RE_CONN_TIMES = 3;
	protected boolean ignoreinput = false;

	protected ReportToDb() throws ClassNotFoundException, SQLException {
		if (null == this.getConf()){
			this.setConf(new Configuration());
		}
	}
	
	protected ReportToDb(Configuration conf) throws ClassNotFoundException, SQLException {
		this.setConf(conf);
	}
	
	protected void reConnect() throws ClassNotFoundException, SQLException{
		if (null != cur_conn){
			System.out.println("Connection failed, re-connecting...");
			try{
				cur_conn.close();
			}catch(Exception ex){}
			cur_conn = null;
		}
		
		int count = 0;
		while(null == cur_conn){
			try{
				dbConf = new DBConfiguration(this.getConf());
				cur_conn = dbConf.getConnection();
				cur_conn.setAutoCommit(false);
			}catch(ClassNotFoundException cnfe){
				if (++count == RE_CONN_TIMES) throw cnfe;
				else System.out.println("Connection failed, trying for times " + (count+1));
				try {
					Thread.sleep(1000*60);
				} catch (InterruptedException e) { }
			}catch(SQLException se){
				if (++count == RE_CONN_TIMES) throw se;
				else System.out.println("Connection failed, trying for times " + (count+1));
				try {
					Thread.sleep(1000*60);
				} catch (InterruptedException e) { }
			}
		}
	}
	
	protected void makeSureConnOK() throws SQLException, ClassNotFoundException{
		if (null == cur_conn) {
			reConnect();
		}else{
			synchronized (cur_conn) {
				try{
					Statement selectSt = cur_conn.createStatement();
					selectSt.setQueryTimeout(6000);
					String sqlselect = "select 1 from dual";
					selectSt.executeQuery(sqlselect);
				}catch(Exception ex){
					reConnect();
				}
			}
		}
	}
	
	public void closeConnection()
		      throws SQLException {
		    cur_conn.close();
	}

	/* 多线程不可入接口, 如果多线程环境请创建多个ReportToDB对象 */
	public static ReportToDb get() throws ClassNotFoundException, SQLException {
		if (null == toDB)
			toDB = new ReportToDb();

		return toDB;
	}
	
	public void setModelName(String modelName){
			this.getConf().set(REPORT_MODEL_NAME, modelName);
	}

	public void deleteRecord(Date startDate, String tableName) throws SQLException {
		int startTime = (int) (startDate.getTime() / 1000);

		Statement delSt = cur_conn.createStatement();
		delSt.setQueryTimeout(6000);
		String sqlDel = "delete from " + tableName + " where timestamp=" + startTime;

		synchronized (cur_conn) {
			if (true != delSt.execute(sqlDel)) {
				System.out.println("deleteRecord success: (" + sqlDel + "), startDate=" + startDate.toString());
			} else {
				System.out.println("deleteRecord fail: (" + sqlDel + "), startDate=" + startDate.toString());
			}
			delSt.close();

			/*
			 * 执行事务之后, 应该尽早提交, 否则会出现mysql的死锁、或锁等待超时现象, 如下: java.sql.SQLException:
			 * Lock wait timeout exceeded; try restarting transactio
			 */
			cur_conn.commit();
		}
	}

	public boolean isDataExists(Date startDate, String tableName) throws SQLException {
		// 默认数据已经存在，以防止数据重复插入
		boolean ret = true;
		int startTime = (int) (startDate.getTime() / 1000);
		Statement selectSt = cur_conn.createStatement();
		selectSt.setQueryTimeout(6000);
		//String sqlselect = "select count(*) from " + tableName + " where timestamp=" + startTime;
		String sqlselect = "select 1 from " + tableName + " where timestamp=" + startTime + " limit 1";
		synchronized (cur_conn) {
			ResultSet res = selectSt.executeQuery(sqlselect);
			if (null != res && res.next()) {
//				String count = res.getString(1);
//				if (StringUtils.isNumeric(count)) {
//					if (0 == count.compareTo("0")) {
//						ret = false;
//					}
//				} else {
//					/* 返回值异常不入库, 打印日志, 后面人工干预 */
//					System.out.println("isDataExists fail: (" + sqlselect + ") return " + count + ", startDate=" + startDate.toString());
//				}
			} else {
				ret = false;
				/* 返回值异常不入库, 打印日志, 后面人工干预 */
				//System.out.println("isDataExists fail: (" + sqlselect + ") return null, startDate=" + startDate.toString());
			}
			selectSt.close();

			cur_conn.commit();
		}
		return ret;
	}

	public int report2Db(Date startDate, List<Path> paths, String tableName, boolean forceupdate, boolean isDist) throws Exception{
		/* 设置时间属性 */
		this.getConf().setInt(REPORT_TIME, (int) (startDate.getTime() / 1000));  
		this.getConf().setStrings(REPORT_TABLE_NAME, tableName);

		List<Path> inputs = Lists.newArrayList();
		FileSystem fs = FileSystem.get(this.getConf());
		for (Path path : paths) {
			if(fs.exists(path) && fs.getFileStatus(path).isDir()) {
				inputs.add(path);
			} else {
				System.out.println("runReport: (" + path.toString() + ") not exist or Dir, cannot be added to inputs!");
			}
		}

		if(inputs.size() <= 0)		{
			if (ignoreinput){
				System.out.println("ignoreinput=true, ReportSource.get().getInputs return empty, skip it!");
				return 0;
			}else{
				throw new Exception("runReport: ReportSource.get().getInputs return empty");
			}
		}
		
		//makeSureConnOK();
		if (isDataExists(startDate, tableName)){
			if (forceupdate) {
				/* 强制更新时, 数据表中若存在该startDate的数据则全部删除 */
				deleteRecord(startDate, tableName);
			} else {
				/* 不强制更新时, 数据表中若存在该startDate时则不用再次入库*/
				System.out.println("Data is already in DB, "+tableName + ", "+startDate);
				return 0;
			}
		}

		
		AvroJob job = ReportJob.get(this.getConf());
		job.setJobName(this.getClass().getName()+ "_"+ tableName +"_"+ FORMAT_OOZIE.format(startDate));
		for (Path path: inputs){
			FileInputFormat.addInputPath(job, path);
		}
		job.setInputFormatClass(AvroPairInputFormat.class);
		job.setMapperClass(ReportToDbC.M.class);
		job.setNumReduceTasks(0);
		
		Path tmpOutput = ReportConstantsUtils.getTmpSegmentDir(ReportConstants.REPORT_OUTPUT);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Object.class);
		job.setOutputValueClass(Object.class);
		TextOutputFormat.setOutputPath(job, tmpOutput);

		if (isDist){
			if (!runJob(job)){
				System.out.println("Report to db error, see job for details");
				return -1;
			}
		}else{
		  TaskAttemptContext taskContext = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
			ReportDbOutputFormat rdf = new ReportDbOutputFormat(tableName);
			RecordWriter<Object, Object> writer = rdf.getRecordWriter(taskContext);
			long lAlgRecordCount = 0;
			ToDbUtils.numRecordErr = 0L;
			Pair<Object, Object> pair = null;
			for (Path input : inputs) {
				MapAvroFile.Reader<Object, Object>[] reads = AvroMapOutputFormat.getReaders(input, this.getConf());
				for (MapAvroFile.Reader<Object, Object> read : reads) {
					while (read.hasNext()) {
						pair = read.next();
						Object rk = pair.key() instanceof UnionData ? ((UnionData) pair.key()).datum : pair.key();
						writer.write(rk, pair.value());
						lAlgRecordCount++;
					}
					read.close();
				}
			}
			/*
			 * AsyncSqlRecordWriter.close原来在mapreduce框架里面调用, 这里要显示调用: 1
			 * sql语句每100条批处理一次, close里面完成剩余的sql语句执行 2 close里面会阻塞等待sql执行线程结束才退出,
			 * 如果不调用主线程会退出, 进程退出
			 */
			writer.close(taskContext);

			System.out.println("runReport: write " + (lAlgRecordCount - ToDbUtils.numRecordErr) +"/" + lAlgRecordCount + 
					"(sucess/total) records to DB from " + inputs.toString() + " at time " + (new Date()).toString());
			System.out.println("Error data rows: " + ToDbUtils.numRecordErr +
					((ToDbUtils.numRecordErr==0)?"; Congratulations！":"; See details in log!"));
		}
		return 0;
	}
	
	public int run(String[] args) throws Exception {
		if (args == null || args.length < 2) {
			System.out.println("Usage: <date> <forceUpdate> <tableName> <paths> [-model modelName]");
			return -1;
		}

		Date startDate = FORMAT_OOZIE.parse(args[0]);
		String[] paths = org.apache.hadoop.util.StringUtils.getTrimmedStrings(args[3]);
		boolean forceupdate = Boolean.valueOf(args[1]);
		String tableName = args[2];
		boolean isDist = false;

        if (args.length > 4) {
        	for (int idx = 4; idx < args.length; idx++) {
        		if ("-model".equalsIgnoreCase(args[idx])) {
        			if (++idx == args.length) {
        				throw new IllegalArgumentException("user name not specified in -username");
        			}
        			setModelName(args[idx]);
        		}else  if ("-dist".equalsIgnoreCase(args[idx])) {
        			if (++idx == args.length) {
        				throw new IllegalArgumentException("whether use distribution not specified in -dist");
        			}
        			isDist = Boolean.parseBoolean(args[idx]);
        		}
        	}
        }
		
		List<Path> inputs = new ArrayList<Path>();
		for (String path:paths){
			inputs.add(new Path(path));
		}
		return report2Db(startDate, inputs, tableName, forceupdate,isDist);
	}

	public static void main(String[] args) throws Exception {
		int ret = OozieToolRunner.run(ReportConfiguration.create(), new ReportToDb(), args);
		System.exit(ret);
	}
}
