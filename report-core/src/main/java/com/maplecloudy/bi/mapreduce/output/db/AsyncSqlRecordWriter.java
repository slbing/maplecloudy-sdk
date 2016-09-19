package com.maplecloudy.bi.mapreduce.output.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.maplecloudy.avro.io.Pair;
import com.maplecloudy.bi.util.LoggingUtils;
import com.maplecloudy.bi.util.todb.ReportToDbAlg;

/**
 * Abstract RecordWriter base class that buffers SqoopRecords to be injected
 * into JDBC SQL PreparedStatements to be executed by the
 * AsyncSqlOutputFormat's background thread.
 *
 * Record objects are buffered before actually performing the INSERT or UPDATE
 * statements; 
 *
 */
public abstract class AsyncSqlRecordWriter<K, V>
extends RecordWriter<K, V> {

	private static final Log LOG = LogFactory.getLog(AsyncSqlRecordWriter.class);
	public static final int MAX_OPS_QUEUE_SIZE = 10;

	// wnagli: modify connection to static
	static protected Connection connection = null;
//	private static final int RE_CONN_TIMES = com.maplecloudy.bi.util.todb.ReportToDb.RE_CONN_TIMES;

	private Configuration conf;

	protected final int rowsPerStmt; // rows to insert per statement.

	// Buffer for records to be put into export SQL statements.
	private List<Pair<K,V>> records;

	// Background thread to actually perform the updates.
	private AsyncSqlOutputFormat.AsyncSqlExecThread execThread;
	private boolean startedExecThread;

	public AsyncSqlRecordWriter(TaskAttemptContext context)
			throws ClassNotFoundException, SQLException {
		this.conf = context.getConfiguration();

		this.rowsPerStmt = conf.getInt(
				AsyncSqlOutputFormat.RECORDS_PER_STATEMENT_KEY,
				AsyncSqlOutputFormat.DEFAULT_RECORDS_PER_STATEMENT);
		int stmtsPerTx = conf.getInt(
				AsyncSqlOutputFormat.STATEMENTS_PER_TRANSACTION_KEY,
				AsyncSqlOutputFormat.DEFAULT_STATEMENTS_PER_TRANSACTION);

		//    DBConfiguration dbConf = new DBConfiguration(conf);
		//    connection = dbConf.getConnection();
		//    connection.setAutoCommit(false);
//		if (null == connection){
//			reConnect();
//		}else{
//			makeSureConnOK();
//		}
		// all connections are managed by ConnManger,
		// we should not new or close a connection.
//		connection = ReportToDbAlg.get(conf).cur_conn;
		setConnectionDefault(context);
		if (null == connection){
			System.out.println("AsyncSqlRecordWriter : Connection is null!!!");
		}

		this.records = new ArrayList<Pair<K,V>>(this.rowsPerStmt);

		this.execThread = new AsyncSqlOutputFormat.AsyncSqlExecThread(connection, stmtsPerTx);
		this.execThread.setDaemon(true);
		this.startedExecThread = false;
	}
	
	public void setConnection(Connection conn){
		connection = conn;
	}
	
	public abstract void setConnectionDefault(TaskAttemptContext context) throws ClassNotFoundException, SQLException;

//	protected void reConnect() throws ClassNotFoundException, SQLException{
//		if (null != connection){
//			System.out.println("Connection failed, re-connecting...");
//			synchronized (connection) {
//				try{
//					connection.close();
//				}catch(Exception ex){}
//				connection = null;
//			}
//		}
//
//		int count = 0;
//		while(null == connection){
//			try{
//				DBConfiguration dbConf = new DBConfiguration(conf);
//				connection = dbConf.getConnection();
//				connection.setAutoCommit(false);
//			}catch(ClassNotFoundException cnfe){
//				if (++count == RE_CONN_TIMES) throw cnfe;
//				else System.out.println("Connection failed, trying for times " + (count+1));
//				try {
//					Thread.sleep(1000*60);
//				} catch (InterruptedException e) { }
//			}catch(SQLException se){
//				if (++count == RE_CONN_TIMES) throw se;
//				else System.out.println("Connection failed, trying for times " + (count+1));
//				try {
//					Thread.sleep(1000*60);
//				} catch (InterruptedException e) {}
//			}
//		}
//	}
//
//	protected void makeSureConnOK() throws SQLException, ClassNotFoundException{
//		synchronized (connection) {
//			try{
//				Statement selectSt = connection.createStatement();
//				selectSt.setQueryTimeout(6000);
//				String sqlselect = "select 1 from dual";
//				selectSt.executeQuery(sqlselect);
//			}catch(Exception ex){
//				reConnect();
//			}
//		}
//	}


	/**
	 * Allow subclasses access to the Connection instance we hold.
	 * This Connection is shared with the asynchronous SQL exec thread.
	 * Any uses of the Connection must be synchronized on it.
	 * @return the Connection object used for this SQL transaction.
	 */
	protected final Connection getConnection() {
		return connection;
	}

	/**
	 * Allow subclasses access to the Configuration.
	 * @return the Configuration for this MapReduc task.
	 */
	protected final Configuration getConf() {
		return this.conf;
	}

	/**
	 * Should return 'true' if the PreparedStatements generated by the
	 * RecordWriter are intended to be executed in "batch" mode, or false
	 * if it's just one big statement.
	 */
	protected boolean isBatchExec() {
		return false;
	}

	/**
	 * Generate the PreparedStatement object that will be fed into the execution
	 * thread. All parameterized fields of the PreparedStatement must be set in
	 * this method as well; this is usually based on the records collected from
	 * the user in the userRecords list.
	 *
	 * Note that any uses of the Connection object here must be synchronized on
	 * the Connection.
	 *
	 * @param userRecords a list of records that should be injected into SQL
	 * statements.
	 * @return a PreparedStatement to be populated with rows
	 * from the collected record list.
	 */
	protected abstract List<PreparedStatement> getPreparedStatement(
			List<Pair<K,V>> userRecords) throws SQLException;

	/**
	 * Takes the current contents of 'records' and formats and executes the
	 * INSERT statement.
	 * @param closeConn if true, commits the transaction and closes the
	 * connection.
	 */
	private void execUpdate(boolean commit, boolean stopThread)
			throws InterruptedException, SQLException {

		if (!startedExecThread) {
			this.execThread.start();
			this.startedExecThread = true;
		}

		List<PreparedStatement>  stmt = null;
		boolean successfulPut = false;
		try {
			if (records.size() > 0) {
				stmt = getPreparedStatement(records);
				this.records.clear();
			}

			// Pass this operation off to the update thread. This will block if
			// the update thread is already performing an update.
			AsyncSqlOutputFormat.AsyncDBOperation op = 
					new AsyncSqlOutputFormat.AsyncDBOperation(stmt, isBatchExec(), commit, stopThread);
			execThread.put(op);
			successfulPut = true; // op has been posted to the other thread.
		} finally {
			if (!successfulPut && null != stmt) {
				// We created a statement but failed to enqueue it. Close it.
				for(PreparedStatement st : stmt){
					st.close();
					com.mysql.jdbc.Connection connMysql = (com.mysql.jdbc.Connection)connection;
					connMysql.setDontTrackOpenResources(true);// special public interface of mysql
					st = null;
				}
			}
		}

		// Check for any previous SQLException. If one happened, rethrow it here.
		SQLException lastException = execThread.getLastError();
		if (null != lastException) {
			LoggingUtils.logAll(LOG, lastException);
			throw lastException;
		}
	}

	@Override
	/** {@inheritDoc} */
	public void close(TaskAttemptContext context)
			throws IOException, InterruptedException {
		try {
			try {
				execUpdate(true, true);
				execThread.join();
			} catch (SQLException sqle) {
				throw new IOException(sqle);
			}

			// If we're not leaving on an error return path already,
			// now that execThread is definitely stopped, check that the
			// error slot remains empty.
			SQLException lastErr = execThread.getLastError();
			if (null != lastErr) {
				throw new IOException(lastErr);
			}
		} finally {
//			try {
//				closeConnection(context);
//			} catch (SQLException sqle) {
//				throw new IOException(sqle);
//			}
		}
	}

//	public static void closeStaticConnection()
//			throws SQLException {
//		connection.close();
//	}
//
//	public void closeConnection(TaskAttemptContext context)
//			throws SQLException {
//		// 静态变量，不再需要关闭
//		//connection.close();
//	}


	private int count = 0;

	@Override
	/** {@inheritDoc} */
	public void write(K key, V value)
			throws InterruptedException, IOException {
		try {
			Pair<K,V> pair =new Pair<K,V>();
			pair.key(key);
			pair.value(value);
			records.add(pair);
			if (records.size() >= this.rowsPerStmt) {
				// TODO wangli
				//execUpdate(false, false);
				// add by liwang4, no limit of queue makes shortage of memory
				while (execThread.getOpsQueueSize() >= MAX_OPS_QUEUE_SIZE){
					Thread.sleep(50);
				}

				if (count%50 == 0){
					System.out.println("Row:"+count*this.rowsPerStmt);
				}
				execUpdate(false, false);
				++count;
			}
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
			throw new IOException(sqlException);
		}
	}
}
