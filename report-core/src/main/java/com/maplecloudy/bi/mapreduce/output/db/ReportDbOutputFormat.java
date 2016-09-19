package com.maplecloudy.bi.mapreduce.output.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.maplecloudy.avro.io.Pair;
import com.maplecloudy.avro.reflect.ReflectDataEx;
import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.model.report.ReportKey;
import com.maplecloudy.bi.model.report.ReportValues;
import com.maplecloudy.bi.report.algorithm.DbOutputAble;
import com.maplecloudy.bi.report.algorithm.ReportAlgorithm;
import com.maplecloudy.bi.util.ReportUtils;
import com.maplecloudy.bi.util.todb.ReportToDbAlg;

/**
 * Update an existing table of data with new value data. This requires a
 * designated 'key column' for the WHERE clause of an UPDATE statement.
 * 
 * Updates are executed en batch in the PreparedStatement.
 * 
 * Uses DBOutputFormat/DBConfiguration for configuring the output.
 */
@SuppressWarnings({"rawtypes"})
public class ReportDbOutputFormat extends
    AsyncSqlOutputFormat<ReportKey,ReportValues> {
  
  @Override
  /** {@inheritDoc} */
  public void checkOutputSpecs(JobContext context) throws IOException {}
  
  @Override
  /** {@inheritDoc} */
  public RecordWriter<ReportKey,ReportValues> getRecordWriter(
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
      AsyncSqlRecordWriter<ReportKey,ReportValues> {
    
    protected HashMap<Class<? extends ReportAlgorithm>,String> tableName = Maps
        .newHashMap();
    // the insert or update values
    protected HashMap<Class<? extends ReportAlgorithm>,TreeSet<String>> values = Maps
        .newHashMap();
    // if the keys have exists, then update with the keys, other insert the keys
    // and values.
    protected HashMap<Class<? extends ReportAlgorithm>,TreeSet<String>> keys = Maps
        .newHashMap();
    
    List<ReportAlgorithm> ras = null;
    
    public UpdateRecordWriter(TaskAttemptContext context)
        throws ClassNotFoundException, SQLException {
      super(context);
      List<ReportAlgorithm> lst = context.getConfiguration().getInstances(
          ReportConstants.REPORT_ALGORITHMS, ReportAlgorithm.class);
      
      for (ReportAlgorithm ra : lst) {
        if (DbOutputAble.class.isAssignableFrom(ra.getClass())) {
          if (ras == null) ras = Lists.newArrayList();
          ras.add(ra);
          keys.put(ra.getClass(), ReportUtils.getKeyNames(ra.getClass()));
          tableName.put(ra.getClass(), ra.getTableName());
          values.put(ra.getClass(), ReportUtils.getAllValueNames(ra.getClass()));
        }
      }
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
        List<Pair<ReportKey,ReportValues>> userRecords) throws SQLException {
      
//      HashMap<Class<? extends ReportAlgorithm>,PreparedStatement> updateStmt = new HashMap<Class<? extends ReportAlgorithm>,PreparedStatement>();
      HashMap<Class<? extends ReportAlgorithm>,PreparedStatement> insertStmt = new HashMap<Class<? extends ReportAlgorithm>,PreparedStatement>();
      List<PreparedStatement> lstmt = Lists.newArrayList();
      // Synchronize on connection to ensure this does not conflict
      // with the operations in the update thread.
      Connection conn = getConnection();
      
      synchronized (conn) {
//        HashMap<Class<? extends ReportAlgorithm>,String> hm = getUpdateStatement();
//        for (Map.Entry<Class<? extends ReportAlgorithm>,String> enty : hm
//            .entrySet()) {
//          PreparedStatement st = conn.prepareStatement(enty.getValue());
//          st.setQueryTimeout(100000);
//          updateStmt.put(enty.getKey(), st);
//          lstmt.add(st);
//        }
//        
        HashMap<Class<? extends ReportAlgorithm>,String> hmin = this.getInsertStatement();
        for (Map.Entry<Class<? extends ReportAlgorithm>,String> enty : hmin.entrySet()) {
          PreparedStatement st = conn.prepareStatement(enty.getValue());
          st.setQueryTimeout(100000);
          insertStmt.put(enty.getKey(), st);
          lstmt.add(st);
        }
      }
      
      // Add a select statement
//      Statement selectSt = conn.createStatement();
//      selectSt.setQueryTimeout(100000);
      // lstmt.add(selectSt);
      
      // Inject the record parameters into the UPDATE and WHERE clauses. This
      // assumes that the update key column is the last column serialized in
      // by the underlying record. Our code auto-gen process for exports was
      // responsible for taking care of this constraint.
      for (Pair<ReportKey,ReportValues> record : userRecords) {
        Class<?> ra = ReportAlgorithm.getAlgorithm(getConf(), record.key().getClass());
        TreeSet<String> updates = keys.get(ra);
        TreeSet<String> rvs = values.get(ra);
//        String sql = "select id from " + tableName.get(ra) + " where ";
//        for (String update : updates) {
//          sql += update + "='"
//              + ReflectDataEx.get().getField(record.key(), update, 0)
//              + "' and ";
//        }
//        sql += "timestamp="
//            + getConf().getInt(ReportConstants.REPORT_TIME,
//                (int) (System.currentTimeMillis() / 1000));
//        ResultSet ret = null;
//        try {
//          ret = selectSt.executeQuery(sql);
//        } catch (Exception e) {
//          System.out.println(sql);
//          selectSt.close();
//          continue;
//        }
//        PreparedStatement pst;
//        if (ret == null || !ret.next()) pst = insertStmt.get(ra);
//        else pst = updateStmt.get(ra);
        PreparedStatement pst = insertStmt.get(ra);
        int i = 1;
        for (String rv : rvs) {
          pst.setString(i, "" + record.value().get(rv));
          i++;
        }
        
        for (String update : updates) {
          boolean isNullStr = false;
          Object tmpV = ReflectDataEx.get().getField(record.key(), update, 0);
          if (null == tmpV){
            try {
                Class tmpC = record.key().getClass().getField(update).getType();
                if (tmpC.equals(String.class)){
                  isNullStr = true;
                }
            } catch (Exception e) {}
          }
          if (!isNullStr)
            pst.setString(i, "" + tmpV);
          else 
            pst.setNull(i, Types.CHAR);
          i++;
        }
        pst.setString(i, ""+getConf().getInt(ReportConstants.REPORT_TIME, 
            (int) (System.currentTimeMillis() / 1000)));
        pst.addBatch();
      }
      
      //selectSt.close();
      return lstmt;
    }
    
    /**
     * @return an INSERT statement suitable for inserting 'numRows' rows.
     */
    protected HashMap<Class<? extends ReportAlgorithm>,String> getInsertStatement() {
      HashMap<Class<? extends ReportAlgorithm>,String> hm = new HashMap<Class<? extends ReportAlgorithm>,String>();
      for (ReportAlgorithm ra : ras) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("INSERT INTO " + tableName.get(ra.getClass()) + " ");
        
        int numSlots;
        numSlots = this.keys.get(ra.getClass()).size()
            + this.values.get(ra.getClass()).size()+1;
        
        sb.append("(");
        boolean first = true;
        for (String col : values.get(ra.getClass())) {
          if (!first) {
            sb.append(", ");
          }
          
          sb.append(col.replace("-", ""));
          first = false;
        }
        for (String col : keys.get(ra.getClass())) {
          if (!first) {
            sb.append(", ");
          }
          
          sb.append(col);
          first = false;
        }
        sb.append(",timestamp) ");
        sb.append("VALUES ");
        
        // generates the (?, ?, ?...) used for each row.
        StringBuilder sbRow = new StringBuilder();
        sbRow.append("(");
        for (int i = 0; i < numSlots; i++) {
          if (i != 0) {
            sbRow.append(", ");
          }
          
          sbRow.append("?");
        }
        sbRow.append(")");
        sb.append(sbRow);
//        System.out.println(sb.toString());
        hm.put(ra.getClass(), sb.toString());
      }
      return hm;
    }
    
    /**
     * @return an UPDATE statement that modifies rows based on a single key
     *         column (with the intent of modifying a single row).
     */
    protected HashMap<Class<? extends ReportAlgorithm>,String> getUpdateStatement() {
      HashMap<Class<? extends ReportAlgorithm>,String> hm = new HashMap<Class<? extends ReportAlgorithm>,String>();
      for (ReportAlgorithm ra : ras) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE " + this.tableName.get(ra.getClass()) + " SET ");
        
        boolean first = true;
        for (String col : this.values.get(ra.getClass())) {
          if (!first) {
            sb.append(", ");
          }
          
          sb.append(col.replace("-", ""));
          sb.append("=?");
          first = false;
        }
        sb.append(" WHERE ");
        first = true;
        for (String key : keys.get(ra.getClass())) {
          if (first) {
            first = false;
          } else {
            sb.append(" AND ");
          }
          sb.append(key).append("=?");
        }
        sb.append(" AND timestamp=?");
        hm.put(ra.getClass(), sb.toString());
//        System.out.println(sb.toString());
      }
      return hm;
    }

	@Override
	public void setConnectionDefault(TaskAttemptContext context)
			throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		connection = ReportToDbAlg.get(context.getConfiguration()).cur_conn;
	}
    
  }
  
}
