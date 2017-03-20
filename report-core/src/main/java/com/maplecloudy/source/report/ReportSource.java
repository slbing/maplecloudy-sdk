package com.maplecloudy.source.report;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.ReportFrequence;
import com.maplecloudy.bi.report.algorithm.ReportAlgorithm;
import com.maplecloudy.bi.util.BucketPath;
import com.maplecloudy.bi.util.ReportConstantsUtils;
import com.maplecloudy.share.util.ConstantsUtils;
import com.maplecloudy.source.Source;
@SuppressWarnings("rawtypes") 
public class ReportSource implements Source{
	private static ReportSource rs = new ReportSource();
	private ReportSource(){}
	public static ReportSource get()
	{
		return rs;
	}
	public final static String REPORT_OUTPUT = ReportConstants.REPORT_OUTPUT;
	public Path getOutput(Date startDate, ReportFrequence frequence)
	{
		Path out = null;
		Path base = new Path(REPORT_OUTPUT, frequence.name());
		switch (frequence) {
		case NONE:
		case Hourly:
			out = new Path(base, BucketPath.escapeString("%Y-%m-%d/%H", startDate));
			break;
		case Daily:
			out = new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
			break;
		case Weekly:
			out = new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
			break;
		case Monthly:
			out = new Path(base, BucketPath.escapeString("%Y-%m", startDate));
			break;
		case Thirtydays:
			out = new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
			break;
		}
		return out;
	}

	public List<Path> getInputs(Date startDate, ReportFrequence frequence, List<Class<? extends ReportAlgorithm>> ras)
	{
		Path out = null;
		Path base = new Path(REPORT_OUTPUT, frequence.name());
		switch (frequence) {
		case NONE:
		case Hourly:
			out = new Path(base, BucketPath.escapeString("%Y-%m-%d/%H", startDate));
			break;
	
		case Daily:
			out = new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
			break;
		case Weekly:
			out = new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
			break;
		case Monthly:
			out = new Path(base, BucketPath.escapeString("%Y-%m", startDate));
			break;
		case Thirtydays:
			out = new Path(base, BucketPath.escapeString("%Y-%m-%d", startDate));
			break;
		}

		/* 得到指定日期下各算法的输出路径 */
		List<Path> lstOut = new ArrayList<Path>();
		for(Class<? extends ReportAlgorithm> ra : ras)
		{
			lstOut.add(new Path(out, ra.getSimpleName()));
		}

		return lstOut;
	}

	public List<Path> getInputs(Date startDate, ReportFrequence frequence, List<Class<? extends ReportAlgorithm>> ras, String userName)
	{
		List<Path> lstPaths = getInputs(startDate, frequence, ras);
		if (StringUtils.isBlank(userName))
		{
			return lstPaths;
		}

		List<Path> inputs = Lists.newArrayList();
		for(Path input : lstPaths)
		{
			inputs.add(new Path(ConstantsUtils.getUserHome(userName), input));
		}

		return inputs;
	}

	public List<Path> getInputsCur(Date startDate, ReportFrequence frequence, List<Class<? extends ReportAlgorithm>> ras, String userName)
	{
		List<Path> lstPaths = getInputs(startDate, frequence, ras);
		List<Path> inputs = Lists.newArrayList();
		if (StringUtils.isBlank(userName)) {
			for(Path input : lstPaths) {
				inputs.add(ReportConstantsUtils.getCurrentDir(input));
			}
		}else{
			for(Path input : lstPaths) {
				inputs.add(new Path(ConstantsUtils.getUserHome(userName), ReportConstantsUtils.getCurrentDir(input)));
			}
		}
		return inputs;
	}

}
