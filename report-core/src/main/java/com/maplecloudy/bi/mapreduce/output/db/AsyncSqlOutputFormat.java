package com.maplecloudy.bi.mapreduce.output.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.SynchronousQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

import com.maplecloudy.avro.mapreduce.output.ExtFileOutputFormat;

/**
 * Abstract OutputFormat class that allows the RecordWriter to buffer up SQL
 * commands which should be executed in a separate thread after enough commands
 * are created.
 * 
 * This supports a configurable "spill threshold" at which point intermediate
 * transactions are committed.
 * 
 * Clients of this OutputFormat must implement getRecordWriter(); the returned
 * RecordWriter is intended to subclass AsyncSqlRecordWriter.
 */
public abstract class AsyncSqlOutputFormat<K,V> extends
    ExtFileOutputFormat<K,V> {
  
  /** conf key: number of rows to export per INSERT statement. */
  public static final String RECORDS_PER_STATEMENT_KEY = "sqoop.export.records.per.statement";
  
  /**
   * conf key: number of INSERT statements to bundle per tx. If this is set to
   * -1, then a single transaction will be used per task. Note that each
   * statement may encompass multiple rows, depending on the value of
   * sqoop.export.records.per.statement.
   */
  public static final String STATEMENTS_PER_TRANSACTION_KEY = "sqoop.export.statements.per.transaction";
  
  /**
   * Default number of records to put in an INSERT statement or other batched
   * update statement.
   */
  public static final int DEFAULT_RECORDS_PER_STATEMENT = 100;
  
  /**
   * Default number of statements to execute before committing the current
   * transaction.
   */
  public static final int DEFAULT_STATEMENTS_PER_TRANSACTION = 100;
  
  /**
   * Value for STATEMENTS_PER_TRANSACTION_KEY signifying that we should not
   * commit until the RecordWriter is being closed, regardless of the number of
   * statements we execute.
   */
  public static final int UNLIMITED_STATEMENTS_PER_TRANSACTION = -1;
  
  private static final Log LOG = LogFactory.getLog(AsyncSqlOutputFormat.class);
  
  @Override
  /** {@inheritDoc} */
  public void checkOutputSpecs(JobContext context) throws IOException {}
  
  @Override
  /** {@inheritDoc} */
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    return new NullOutputCommitter();
  }
  
  /**
   * Represents a database update operation that should be performed by an
   * asynchronous background thread. AsyncDBOperation objects are immutable.
   * They MAY contain a statement which should be executed. The statement may
   * also be null.
   * 
   * They may also set 'commitAndClose' to true. If true, then the executor of
   * this operation should commit the current transaction, even if stmt is null,
   * and then stop the executor thread.
   */
  public static class AsyncDBOperation {
    private final List<PreparedStatement> stmt;
    private final boolean isBatch;
    private final boolean commit;
    private final boolean stopThread;
    
    /**
     * Create an asynchronous database operation.
     * 
     * @param s
     *          the statement, if any, to execute.
     * @param batch
     *          is true if this is a batch PreparedStatement, or false if it's a
     *          normal singleton statement.
     * @param commit
     *          is true if this statement should be committed to the database.
     * @param stopThread
     *          if true, the executor thread should stop after this operation.
     */
    public AsyncDBOperation(List<PreparedStatement> s, boolean batch,
        boolean commit, boolean stopThread) {
      this.stmt = s;
      this.isBatch = batch;
      this.commit = commit;
      this.stopThread = stopThread;
    }
    
    /**
     * @return a statement to run as an update.
     */
    public List<PreparedStatement> getStatement() {
      return stmt;
    }
    
    /**
     * @return true if the executor should commit the current transaction. If
     *         getStatement() is non-null, the statement is run first.
     */
    public boolean requiresCommit() {
      return this.commit;
    }
    
    /**
     * @return true if the executor should stop after this command.
     */
    public boolean stop() {
      return this.stopThread;
    }
    
    /**
     * @return true if this is a batch SQL statement.
     */
    public boolean execAsBatch() {
      return this.isBatch;
    }
  }
  
  /**
   * A thread that runs the database interactions asynchronously from the
   * OutputCollector.
   */
  public static class AsyncSqlExecThread extends Thread {
    
    private final Connection conn; // The connection to the database.
    private SQLException err; // Error from a previously-run statement.
    
    // How we receive database operations from the RecordWriter.
    private SynchronousQueue<AsyncDBOperation> opsQueue;
    
    protected int curNumStatements; // statements executed thus far in the tx.
    protected final int stmtsPerTx; // statements per transaction.
    
    /**
     * Create a new update thread that interacts with the database.
     * 
     * @param conn
     *          the connection to use. This must only be used by this thread.
     * @param stmtsPerTx
     *          the number of statements to execute before committing the
     *          current transaction.
     */
    public AsyncSqlExecThread(Connection conn, int stmtsPerTx) {
      this.conn = conn;
      this.err = null;
      this.opsQueue = new SynchronousQueue<AsyncDBOperation>();
      this.stmtsPerTx = stmtsPerTx;
    }
    
    public void run() {
      while (true) {
        AsyncDBOperation op = null;
        try {
          op = opsQueue.take();
        } catch (InterruptedException ie) {
          LOG.warn("Interrupted retrieving from operation queue: "
              + StringUtils.stringifyException(ie));
          continue;
        }
        
        if (null == op) {
          // This shouldn't be allowed to happen.
          LOG.warn("Null operation in queue; illegal state.");
          continue;
        }
        
        List<PreparedStatement> stmt = op.getStatement();
        // Synchronize on the connection to ensure it does not conflict
        // with the prepareStatement() call in the main thread.
        synchronized (conn) {
          try {
            if (null != stmt) {
              if (op.execAsBatch()) {
                for (PreparedStatement st : stmt)
                  st.executeBatch();
              } else {
                for (PreparedStatement st : stmt)
                  st.execute();
              }
              for (PreparedStatement st : stmt)
                st.close();
              
              /* ****************
               * 在MySQL jdbc 5.1.6里，默认情况下，如果一个Connection永远不掉用close，即使你每一个Statement, ResultSet
			   * 调用了close，仍然会有内存泄漏，换句话说，Statement的close没有把自己的资源释放干净，Statement会在对应的
				*connection里有缓存   
				***************            */
              /* ************
               * 下面这句很重要，具体作用就是让  Statement 每次close的时候通知Connection把缓存的Statement对象释放掉，这样就释放干净了
               ***************/
              com.mysql.jdbc.Connection connMysql = (com.mysql.jdbc.Connection)conn;
              connMysql.setDontTrackOpenResources(true);// special public interface of mysql
              
              stmt = null;
              this.curNumStatements++;
            }
            
            if (op.requiresCommit()
                || (curNumStatements >= stmtsPerTx && stmtsPerTx != UNLIMITED_STATEMENTS_PER_TRANSACTION)) {
              LOG.debug("Committing transaction of " + curNumStatements
                  + " statements");
              this.conn.commit();
              this.curNumStatements = 0;
            }
          } catch (SQLException sqlE) {
            sqlE.printStackTrace();
            setLastError(sqlE);
          } finally {
            // Close the statement on our way out if that didn't happen
            // via the normal execution path.
            if (null != stmt) {
              try {
                for (PreparedStatement st : stmt)
                  st.close();
              } catch (SQLException sqlE) {
                setLastError(sqlE);
              }
            }
            
            // Always check whether we should end the loop, regardless
            // of the presence of an exception.
            if (op.stop()) {
              return;
            }
          } // try .. catch .. finally.
        } // synchronized (conn)
      }
    }
    
    /**
     * Allows a user to enqueue the next database operation to run. Since the
     * connection can only execute a single operation at a time, the put()
     * method may block if another operation is already underway.
     * 
     * @param op
     *          the database operation to perform.
     */
    public void put(AsyncDBOperation op) throws InterruptedException {
      opsQueue.put(op);
    }
    
    public int getOpsQueueSize(){
    	if (null == opsQueue){
    		return 0;
    	}
    	return opsQueue.size();
    }
    
    /**
     * If a previously-executed statement resulted in an error, post it here. If
     * the error slot was already filled, then subsequent errors are squashed
     * until the user calls this method (which clears the error slot).
     * 
     * @return any SQLException that occurred due to a previously-run statement.
     */
    public synchronized SQLException getLastError() {
      SQLException e = this.err;
      this.err = null;
      return e;
    }
    
    private synchronized void setLastError(SQLException e) {
      if (this.err == null) {
        // Just set it.
        LOG.error("Got exception in update thread: "
            + StringUtils.stringifyException(e));
        this.err = e;
      } else {
        // Slot is full. Log it and discard.
        LOG.error("SQLException in update thread but error slot full: "
            + StringUtils.stringifyException(e));
      }
    }
  }
}
