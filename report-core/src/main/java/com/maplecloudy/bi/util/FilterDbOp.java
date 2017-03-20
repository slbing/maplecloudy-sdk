package com.maplecloudy.bi.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

import com.maplecloudy.bi.mapreduce.output.db.DBConfiguration;
import com.maplecloudy.bi.util.todb.ConnManger;
import com.maplecloudy.bi.util.todb.DBHelper;

public class FilterDbOp{
	
	private DBHelper dbHelper = null;
	ConnManger connManager = null;

	public FilterDbOp(Configuration conf) throws ClassNotFoundException, SQLException, IOException {
		connManager = new ConnManger();
		Connection conn = connManager.getAlgConn("com.maplecloudy.bi.report.algorithm.SvcPerfOfSess");
//		DBConfiguration dbconf = new DBConfiguration(conf);
//		Connection conn = dbconf.getConnection();
		dbHelper = new DBHelper(conn);
	}
	
	public void closeConn() throws SQLException{
//		if (null != dbHelper){
//			dbHelper.closeConn();
//		}
		if (null != connManager){
			connManager.closeConns();
		}
	}
	
	public String getFilterApps(String filter_name) throws SQLException{
		StringBuffer sb = new StringBuffer();
		ResultSet rset = null;
		String sql = "select app_ids from app_filter where filter_name='"+filter_name+"'";
		rset = dbHelper.execQuery(sql);
		boolean isFirst = true;
		while(rset.next()){
			if (!isFirst){
				sb.append(FilterUtil.FILTER_SEG);
			}
			sb.append(rset.getString(1));
			isFirst =false;
		}
		return sb.toString();
	}

	public String getFilterComps(String filter_name) throws SQLException{
		StringBuffer sb = new StringBuffer();
		ResultSet rset = null;
		String sql = "select comps from comp_filter where filter_name='"+filter_name+"'";
		rset = dbHelper.execQuery(sql);
		boolean isFirst = true;
		while(rset.next()){
			if (!isFirst){
				sb.append(FilterUtil.FILTER_SEG);
			}
			sb.append(rset.getString(1));
			isFirst =false;
		}
		return sb.toString();
	}
	
	public String getFilterLayers(String filter_name) throws SQLException{
		StringBuffer sb = new StringBuffer();
		ResultSet rset = null;
		String sql = "select layers from lay_filter where filter_name='"+filter_name+"'";
		rset = dbHelper.execQuery(sql);
		boolean isFirst = true;
		while(rset.next()){
			if (!isFirst){
				sb.append(FilterUtil.FILTER_SEG);
			}
			sb.append(rset.getString(1));
			isFirst =false;
		}
		return sb.toString();
	}

	
	
}
