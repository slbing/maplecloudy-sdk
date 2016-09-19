package com.maplecloudy.bi.util.todb;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Lists;
import com.maplecloudy.bi.ReportFrequence;
import com.maplecloudy.bi.report.algorithm.ReportAlgorithm;
import com.maplecloudy.bi.util.ReportConfiguration;
import com.maplecloudy.bi.util.ReportConstantsUtils;
import com.maplecloudy.bi.util.ReportUtils;
import com.maplecloudy.oozie.main.OozieToolRunner;
import com.maplecloudy.source.report.ReportSource;

@SuppressWarnings({ "rawtypes" })
public class ReportToDbAlg extends ReportToDb {

	private ReportToDbAlg(Configuration conf) throws ClassNotFoundException, SQLException {
		super(conf);
	}
	
	private ReportToDbAlg() throws ClassNotFoundException, SQLException {
		super();
	}

	private static ReportToDbAlg toDB = null;
	ReportFrequence minFreq = ReportFrequence.Hourly;
	boolean isDist = false;
	boolean isDbUserDef = false;

	/* 多线程不可入接口, 如果多线程环境请创建多个ReportToDB对象 */
	public static ReportToDbAlg get(Configuration conf) throws ClassNotFoundException, SQLException {
		if (null == toDB)
			toDB = new ReportToDbAlg(conf);

		return toDB;
	}

	public static ReportToDbAlg get() throws ClassNotFoundException, SQLException {
		if (null == toDB)
			toDB = new ReportToDbAlg();

		return toDB;
	}
	
	public void setDbUserDef(boolean isUserDef){
		this.isDbUserDef = isUserDef;
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
		for (Class<? extends ReportAlgorithm> ra : ras) {
			deleteRecord(startDate, getTableName(ra, freq));
		}
	}

	void deleteRecord(Date startDate, ReportFrequence freq, String[] algorithms) throws ClassNotFoundException, SQLException {
		List<Class<? extends ReportAlgorithm>> ras = ReportUtils.getAlgorithmClasses(algorithms);
		deleteRecord(startDate, freq, ras);
	}

	List<Class<? extends ReportAlgorithm>> updateAlgorithms(Date startDate, ReportFrequence freq, List<Class<? extends ReportAlgorithm>> ras) throws ClassNotFoundException, IOException, SQLException {
		List<Class<? extends ReportAlgorithm>> lstRas = Lists.newArrayList();
		for (Class<? extends ReportAlgorithm> ra : ras) {
			ReportAlgorithm raObject = ReflectionUtils.newInstance(ra, this.getConf());
			String tableName = raObject.getTableName();

			/* 表中当前没有该事件戳的记录, 则需要将algorithm入库 */
			if (!isDataExists(startDate, tableName)){
				lstRas.add(ra);
			}
		}
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
		this.getConf().setInt(REPORT_TIME, (int) (startDate.getTime() / 1000));
		this.getConf().set(REPORT_FREQUENCE, freq.name());

		List<Class<? extends ReportAlgorithm>> useRas = ras;

		ReportAlgorithm.setReportAlgorithms(this.getConf(), useRas);
		for (Class<? extends ReportAlgorithm> ra : ras){
			
			if (isDbUserDef){
				makeSureConnOK();
			}else{
				cur_conn = ConnManger.get().getAlgConn(ra);
			}
			// list中只有一个ReportAlgorithm， 这里使用list只是为了兼容旧接口
			List<Class<? extends ReportAlgorithm>> ras_single = new ArrayList<Class<? extends ReportAlgorithm>>();
			ras_single.add(ra);
			List<Path> inputs = ReportSource.get().getInputsCur(startDate, freq, ras_single, userName);

			int ret = report2Db(startDate, inputs, getTableName(ra, freq), forceupdate, isDist);
			if (0 != ret){
				System.out.println("ReportToDb failed for: ");
				System.out.println("\tstartDate:" + startDate.toString());
				System.out.println("\tinputs:"+inputs);
				System.out.println("\ttableName:"+getTableName(ra,freq));
				System.out.println("\tforceUpdate:"+forceupdate);
			}
			System.gc();
		}
		
		if (isDbUserDef){
			closeConnection();
		}else{
			ConnManger.get().closeConns();
		}
		
		return 0;
	}

	public int report2Db(Date startDate, ReportFrequence freq, boolean forceupdate, String[] algorithms) throws Exception {
		List<Class<? extends ReportAlgorithm>> ras = ReportUtils.getAlgorithmClasses(algorithms);
		return report2Db(startDate, freq, forceupdate, ras);
	}

    protected ReportFrequence getMinFreq(){
    	return minFreq;
    }
    
    public void setIsDist(boolean dist){
    	this.isDist = dist;
    }

	public int runLoop(Date startDate, ReportFrequence freq, boolean forceupdate, String[] algorithms) throws Exception {
		int iRet = 0;
		if (getMinFreq() == freq || ReportFrequence.NONE == freq) {
			iRet = report2Db(startDate, getMinFreq(), forceupdate, algorithms);
		} else if (ReportFrequence.Thirtydays == freq){
			iRet = report2Db(startDate, freq, forceupdate, algorithms);
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
		} else if (ReportFrequence.Monthly == freq || ReportFrequence.Weekly == freq) {
			Date endDate = ReportConstantsUtils.getEdndate(startDate, freq);
			Calendar start = Calendar.getInstance();
			Calendar end = Calendar.getInstance();
			end.setTime(endDate);
			start.setTime(startDate);
			while (start.before(end)) {
				iRet = runLoop(start.getTime(), 
						(ReportFrequence.Thirtydays==getMinFreq())?ReportFrequence.Thirtydays:ReportFrequence.Daily, 
								forceupdate, algorithms);
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
					"> <forceupdate> <algorithms> <isLoop> [-username u] [-model modelName] [-minFreq mf]");
			return -1;
		}
		ReportToDbAlg.toDB = this;

		Date startDate = FORMAT_OOZIE.parse(args[0]);
		ReportFrequence freq = ReportFrequence.valueOf(args[1]);
		String[] algorithms = null;//ReportConstantsUtils.getAlgorithms(freq);
		algorithms = org.apache.hadoop.util.StringUtils.getTrimmedStrings(args[3]);
		boolean forceupdate = Boolean.valueOf(args[2]);
		boolean isLoop = Boolean.valueOf(args[4]);
		
		if (args.length > 5) {
			for (int idx = 5; idx < args.length; idx++) {
				if ("-username".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -username");
					}
					userName = args[idx];
				}else if ("-model".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -username");
					}
					setModelName(args[idx]);
				}else if ("-minFreq".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -username");
					}
					minFreq = ReportFrequence.valueOf(args[idx]);
				}else if ("-dist".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -username");
					}
					// do not support distribute-report any more!!
					//isDist = Boolean.parseBoolean(args[idx]);
				}else if ("-ignoreinput".equalsIgnoreCase(args[idx])) {
					if (++idx == args.length) {
						throw new IllegalArgumentException("user name not specified in -username");
					}
					ignoreinput = Boolean.parseBoolean(args[idx]);
				}else if ("-dbuserdef".equalsIgnoreCase(args[idx])) {
					isDbUserDef = true;
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
		int ret = OozieToolRunner.run(ReportConfiguration.create(), new ReportToDbAlg(), args);
		System.exit(ret);
	}
}
