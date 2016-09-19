package com.maplecloudy.flume.channel;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class FileChannelPlus extends BasicChannelSemantics {
  private static final Logger LOG = LoggerFactory
      .getLogger(FileChannelPlus.class);
  public final static int DEFAULT_ROLL_COUNT = 1000000;
  private File dataDir;
  private File checkpointDir;
  private int rollCount;
  private ChannelPlusCounter channelCounter;
  LogPlus logPlus;
  private final ThreadLocal<FileBackedTransaction> transactions = new ThreadLocal<FileBackedTransaction>();
  
  private int logFileKeepCount;
  
  @Override
  protected BasicTransactionSemantics createTransaction() {
    FileBackedTransaction trans = transactions.get();
    if (trans != null && !trans.isClosed()) {
      Preconditions.checkState(
          false,
          "Thread has transaction which is still open: "
              + trans.getStateAsString());
    }
    trans = new FileBackedTransaction(logPlus, getName(), channelCounter);
    transactions.set(trans);
    return trans;
  }
  
  @Override
  public void configure(Context context) {
    String homePath = System.getProperty("user.home").replace('\\', '/');
    String strDataDir = context.getString(
        FileChannelPlusConfiguration.DATA_DIR, homePath
            + "/.flume/file-channel/data");
    String strCheckpointDir = context.getString(
        FileChannelPlusConfiguration.CHECKPOINT_DIR, homePath
            + "/.flume/file-channel/checkpoint");
    checkpointDir = new File(strCheckpointDir);
    dataDir = new File(strDataDir);
    rollCount = context.getInteger(FileChannelPlusConfiguration.ROLL_COUNT,
        DEFAULT_ROLL_COUNT);
    logFileKeepCount = context.getInteger(
        FileChannelPlusConfiguration.LOGFILE_KEEP_COUNT, 2);
    
    if (channelCounter == null) {
      channelCounter = new ChannelPlusCounter(getName());
    }
  }
  
  @Override
  public synchronized void start() {
    LOG.info("Starting {}...", this);
    try {
      logPlus = new LogPlus(getName(), checkpointDir, dataDir, rollCount,
          logFileKeepCount);
      channelCounter.start();
      super.start();
    } catch (Throwable t) {
      LOG.error("Failed to start the file channel " + getName(), t);
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
  }
  
  @Override
  public synchronized void stop() {
    LOG.info("Stopping {}...", this);
    try {
      logPlus.shutdownWorker();
      logPlus.close();
    } catch (Exception e) {
      LOG.error("Error while trying to close the log.", e);
      Throwables.propagate(e);
    }
    channelCounter.stop();
    super.stop();
  }
  
  static class FileBackedTransaction extends BasicTransactionSemantics {
    private final LogPlus log;
    private final String channelNameDescriptor;
    private final ChannelPlusCounter channelCounter;
    private int fileID;
    private long eventID;
    int takenum = 0;
    int putnum = 0;
    
    public FileBackedTransaction(LogPlus log, String name,
        ChannelPlusCounter counter) {
      this.log = log;
      
      channelNameDescriptor = "[channel=" + name + "]";
      this.channelCounter = counter;
    }
    
    private boolean isClosed() {
      return State.CLOSED.equals(getState());
    }
    
    private String getStateAsString() {
      return String.valueOf(getState());
    }
    
    @Override
    protected void doBegin() {
                                                     //取得cpi中的各种id
      fileID = log.cpi.readerFileID.get();
      eventID = log.cpi.readerEventID.get();
    }
    
    @Override
    protected void doPut(Event event) throws InterruptedException {
                                    //调用一次，counter增加一个计数，由incrementEventPutAttemptCount实现
      channelCounter.incrementEventPutAttemptCount();
      try {
        log.put(event);             //调用LogPlus中的put
      } catch (IOException e) {
        throw new ChannelException("Take failed due to IO error "
            + channelNameDescriptor, e);
      }
      putnum++;
    }
    
    @Override
    protected Event doTake() throws InterruptedException {
                                     //调用一次，counter增加一个计数，由incrementEventTakeAttemptCount实现
      channelCounter.incrementEventTakeAttemptCount();
      try {
        Event event = log.take();
        if (event != null) takenum++;
        return event;
      } catch (IOException e) {
        throw new ChannelException("Take failed due to IO error "
            + channelNameDescriptor, e);
      }
    }
    
    @Override
    protected void doCommit() throws InterruptedException {
      if (putnum > 0) {
        try {
          log.flushWriter();
         // log.cpi.save();
        } catch (IOException e) {
          throw new ChannelException("commit failed due to IO error "
              + channelNameDescriptor, e);
        }
      }
      channelCounter.addToEventPutSuccessCount(putnum);
      channelCounter.addToEventTakeSuccessCount(takenum);
      channelCounter.setChannelSize(log.cpi.writerFlushID.get()
          - log.cpi.readerEventID.get());
    }
    
    @Override
    protected void doRollback() throws InterruptedException {
      if (takenum > 0) {
        try {
          log.restReader(fileID, eventID);
         // log.cpi.save();
        } catch (EOFException e) {
          throw new ChannelException("Take failed due to IO error "
              + channelNameDescriptor, e);
        } catch (IOException e) {
          throw new ChannelException("Take failed due to IO error "
              + channelNameDescriptor, e);
        }
      }
    }
  }
}
