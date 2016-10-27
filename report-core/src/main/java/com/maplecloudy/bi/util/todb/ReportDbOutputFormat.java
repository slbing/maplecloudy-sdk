package com.maplecloudy.bi.util.todb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;
import com.maplecloudy.avro.io.Pair;
import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.mapreduce.output.db.AsyncSqlOutputFormat;
import com.maplecloudy.bi.mapreduce.output.db.AsyncSqlRecordWriter;
import com.maplecloudy.bi.model.report.ReportKey;
import com.maplecloudy.bi.model.report.ReportValues;

/**
 * Update an existing table of data with new value data. This requires a
 * designated 'key column' for the WHERE clause of an UPDATE statement.
 * 
 * Updates are executed en batch in the PreparedStatement.
 * 
 * Uses DBOutputFormat/DBConfiguration for configuring the output.
 */
public class ReportDbOutputFormat extends
AsyncSqlOutputFormat<Object,Object> {

	TableImportInfo tii=null;
	String tableName = null;

	public ReportDbOutputFormat(String tn) {
		super();
		this.tableName = tn;
	}
	public ReportDbOutputFormat() {
		super();
	}

	@Override
	/** {@inheritDoc} */
	public void checkOutputSpecs(JobContext context) throws IOException {}

	@Override
	/** {@inheritDoc} */
	public RecordWriter<Object,Object> getRecordWriter(
			TaskAttemptContext context) throws IOException {
		try {
			return new UpdateRecordWriter(context);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	/**
	 * RecordWriter to write the output to UPDATE statements modifying rows in the
	 * database.
	 */
	public class UpdateRecordWriter extends
	AsyncSqlRecordWriter<Object,Object> {

		private Connection conn;
		private ExceptionDbLogWriter dbLog;
		private Configuration conf;
		private Map<Class<? extends ReportKey>, GetRowDefalut.GetRowReportKeyValue> mapGetRow = 
				new HashMap<Class<? extends ReportKey>, GetRowDefalut.GetRowReportKeyValue>();


		public UpdateRecordWriter(TaskAttemptContext context)
				throws ClassNotFoundException, SQLException {
			super(context);
			conn = getConnection();
			dbLog = new ExceptionDbLogWriter(conn);
			conf = context.getConfiguration();
			tii = new TableImportInfo(tableName, conn);
		}

		@Override
		/** {@inheritDoc} */
		protected boolean isBatchExec() {
			// We use batches here.
			return true;
		}

		@Override
		/** {@inheritDoc} */
		protected List<PreparedStatement> getPreparedStatement(
				List<Pair<Object,Object>> userRecords) throws SQLException {

			List<PreparedStatement> lstmt = Lists.newArrayList();
			// Synchronize on connection to ensure this does not conflict
			// with the operations in the update thread.

			int timestamp = conf.getInt(ReportConstants.REPORT_TIME, (int)((new Date()).getTime()/1000));
			Map<String, Object>  row = null;
			for (Pair<Object,Object> record : userRecords) {

				if (record.key() instanceof ReportKey && record.value() instanceof ReportValues){
					ReportKey rk = (ReportKey)record.key();
					// Actually, rk.getClass() is unique, so mapGetRow should have only one key-value
					GetRowDefalut.GetRowReportKeyValue grkv = mapGetRow.get(rk.getClass());
					if (null == grkv){
						grkv = new GetRowDefalut.GetRowReportKeyValue(conf, rk.getClass());
						mapGetRow.put(rk.getClass(), grkv);
					}
					row = grkv.getRow(rk,(ReportValues)record.value(),row);
				}else if (record.value() instanceof AvroValueType){
					row = ((AvroValueType)record.value()).getRow(row);
				}else{
					ToDbUtils.numRecordErr ++;
					System.out.println("Unsupported key-value type:"+record.key().getClass().getName()+"-"+record.value().getClass().getName());
					continue;
				}

				try{
					tii.bindRow(row);
				}catch(SQLException ex){
					ToDbUtils.numRecordErr ++;
					String context = "[Row Data]:" + row;
					String errorInfo = ex.getMessage();
					String type;
					if (record.key() instanceof ReportKey) type = record.key().getClass().getName();
					else type = record.value().getClass().getName();
					dbLog.write(type, context, errorInfo, conf.get(ReportConstants.REPORT_MODEL_NAME),timestamp);
				}
			}

			//selectSt.close();
			lstmt.add(tii.getPst());
			return lstmt;
		}

		@Override
		public void setConnectionDefault(TaskAttemptContext context)
				throws ClassNotFoundException, SQLException {
		  ReportToDbAlg.get(context.getConfiguration()).makeSureConnOK();
			// TODO Auto-generated method stub
			connection = ReportToDbAlg.get(context.getConfiguration()).cur_conn;
		}

//		private HashMap<Class<? extends ReportAlgorithm>, String> getInsertStatement() {
//			String sql = tii.getBatchSql();
//			HashMap<Class<? extends ReportAlgorithm>, String> map = new HashMap<Class<? extends ReportAlgorithm>, String>();
//			map.put(null, sql);
//			return map;
//		}

	}

}