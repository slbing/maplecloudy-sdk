package com.maplecloudy.bi.util.todb;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;

import com.maplecloudy.avro.reflect.ReflectDataEx;
import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.model.report.ReportKey;
import com.maplecloudy.bi.model.report.ReportValues;
import com.maplecloudy.bi.report.algorithm.DbOutputAble;
import com.maplecloudy.bi.report.algorithm.ReportAlgorithm;
import com.maplecloudy.bi.util.ReportUtils;

public class GetRowDefalut {

	public static class GetRowReportKeyValue{
		protected TreeSet<String> valueFields = null;
		protected TreeSet<String> keyFields = null;
		//protected String tableName = null;
		protected int timestamp;

		/**
		 * @param conf Configuration，需要指定 {@link ReportConstants.REPORT_ALGORITHMS} 
		 * 和 {@link ReportConstants.REPORT_TIME}
		 * @param rk the class of ReportKey
		 */
		@SuppressWarnings("rawtypes")
		public GetRowReportKeyValue(Configuration conf, Class<? extends ReportKey> rk) {
			Class<? extends ReportAlgorithm>  cra = ReportAlgorithm.getAlgorithm(conf, rk);
			//ReportAlgorithm ra = cra.getConstructor().newInstance();
			if (DbOutputAble.class.isAssignableFrom(cra)) {
				keyFields = ReportUtils.getKeyNames(cra);
				//tableName = ra.getTableName();
				valueFields = ReportUtils.getAllValueNames(cra);
			}
			timestamp = conf.getInt(ReportConstants.REPORT_TIME, (int) (System.currentTimeMillis() / 1000));
		}

		public Map<String,Object> getRow(ReportKey rk, ReportValues rvs, Map<String,Object> reuse){
			Map<String,Object> ret;
			if (null == reuse){
				ret = new HashMap<String, Object>();
			}else{
				ret = reuse;
			}
			 
			for (String vf : valueFields) {
				// 数据库字段不区分大小写，统一使用小写
				String vf_low = vf.toLowerCase().replace("-", "");
				ret.put(vf_low, rvs.get(vf));
			}

			for (String kf : keyFields) {
//				boolean isNullStr = false;
				Object tmpV = ReflectDataEx.get().getField(rk, kf, 0);
				ret.put(kf.toLowerCase(),tmpV);
//				if (null == tmpV){
//					try {
//						Class tmpC = rk.getClass().getField(kf).getType();
//						if (tmpC.equals(String.class)){
//							isNullStr = true;
//						}
//					} catch (Exception e) {}
//				}
//				if (!isNullStr){
//					ret.put(kf.toLowerCase(), tmpV);
//				}
			}

			ret.put("timestamp", timestamp);

			return ret;
		}
	}

}
