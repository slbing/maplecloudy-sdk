package com.maplecloudy.flume.channel;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.maplecloudy.avro.io.Pair;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LogPlus {
  public static final String PREFIX = "log-";
  private static final Logger LOGGER = LoggerFactory.getLogger(LogPlus.class);
  private static final int MIN_NUM_LOGS = 2;
  // for reader
  LogFilePlus.Reader currentReader;
  private final AtomicInteger nextFileID = new AtomicInteger(0);
  private final File checkpointDir;
  private final File logDir;
  public CheckpointInfo cpi;
  LogFilePlus.Writer currentWriter;
  private final String channelNameDescriptor;
  
  private final ScheduledExecutorService workerExecutor;
  
  private final List<File> pendingDeletes = Lists.newArrayList();
  
  private int rollCount;
  
  public int logFileKeepCount;
  
  public LogPlus(String name, File checkpointDir, File logDir, int rollCount,
      int logFileKeepCount) throws IOException {
    
    Preconditions.checkNotNull(checkpointDir, "checkpointDir");
    
    Preconditions.checkArgument(
        checkpointDir.isDirectory() || checkpointDir.mkdirs(), "CheckpointDir "
            + checkpointDir + " could not be created");
    
    Preconditions.checkNotNull(logDir, "logDir");
    
    Preconditions.checkArgument(name != null && !name.trim().isEmpty(),
        "channel name should be specified");
    // Preconditions.checkArgument(logFileKeepCount > 1, "");
    this.rollCount = rollCount;
    this.logFileKeepCount = logFileKeepCount;
    this.channelNameDescriptor = "[channel=" + name + "]";
    Preconditions.checkArgument(logDir.isDirectory() || logDir.mkdirs(),
        "LogDir " + logDir + " could not be created");
    this.checkpointDir = checkpointDir;
    cpi = CheckpointInfo.get(checkpointDir);
    this.logDir = logDir;
    
    nextFileID.set(0);
    boolean hasOldFile = false;
    
    List<File> dataFiles = LogUtils.getLogs(logDir);
    LogUtils.sort(dataFiles);
    for (File file : LogUtils.getLogs(logDir)) {
      hasOldFile = true;
      int id = LogUtils.getIDForFile(file);
      // dataFiles.add(file);
      nextFileID.set(Math.max(nextFileID.get(), id));
      if (id == cpi.readerFileID.get()) {
        
        currentReader = new LogFilePlus.Reader(new File(logDir, PREFIX + id),
            id, cpi.readerEventID.get());
      }
    }
    // writer allways to a new file
    if (hasOldFile) {
      int id = nextFileID.incrementAndGet();
      currentWriter = new LogFilePlus.Writer(new File(logDir, PREFIX + id), id,
          rollCount);
      cpi.writerFileID.set(id);
      if (currentReader == null) {//如果没有找到read文件，则从当前最小编号的logfile开始读
        File minFile = dataFiles.get(0);
        int minFileID = LogUtils.getIDForFile(minFile);
        currentReader = new LogFilePlus.Reader(minFile, minFileID,
            cpi.readerEventID.get());
        cpi.readerFileID.set(minFileID);
      }
    } else {//没有旧文件
      cpi.writerFileID.set(nextFileID.get());
      currentWriter = new LogFilePlus.Writer(new File(logDir, PREFIX
          + nextFileID.get()), nextFileID.get(), rollCount);
      if (currentReader == null) {
        currentReader = new LogFilePlus.Reader(new File(logDir, PREFIX
            + nextFileID.get()), nextFileID.get(), cpi.readerEventID.get());
        cpi.readerFileID.set(nextFileID.get());
      }
    }
    LOGGER.info("Current read on " + currentReader.getFile());
    LOGGER.info("Current writer on " + currentWriter.getFile());
    workerExecutor = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("Log-BackgroundWorker-" + name).build());
    workerExecutor.scheduleWithFixedDelay(new BackgroundWorker(this), 10000,
        10000, TimeUnit.MILLISECONDS);
    
  }
  
  private Object readLock = new Object();
  
  public Event take() throws IOException {
    synchronized (readLock) {
      Pair<Long,Event> pair = null;
      if (cpi.readerEventID.get() >= cpi.writerFlushID.get()) return null;
      try {
        long perEventId = cpi.readerEventID.get();
        if (this.currentReader.hasNext(perEventId)) {
          pair = this.currentReader.next();
          // System.out.println("cur position:"+
          // this.currentReader.getPosition());
          // cpi.readerFileOffset.set(this.currentReader.previousSync());
          
          cpi.readerEventID.set(pair.key());
          if (pair.key() != (perEventId + 1)) {
            LOGGER.warn("Discontinuous eventID, per:" + perEventId + ",cur:"
                + pair.key());
          }
          return pair.value();
        } else {
          // roll to next file to read
          if (cpi.writerFlushID.get() > cpi.readerEventID.get()
              || cpi.writerFileID.get() > cpi.readerFileID.get()) {
            gotToNextReader();
            return null;
          }
        }
      } catch (Exception e) {
        LOGGER.warn("file maybe not been regular colsed:"
            + currentReader.getFile());
        if (cpi.writerFlushID.get() > cpi.readerEventID.get()
            || cpi.writerFileID.get() > cpi.readerFileID.get()) {
          gotToNextReader();
          return null;
        } else return null;
      }
      return null;
    }
  }
  
  private Object writeLock = new Object();
  
  public void put(Event event) throws IOException {
    synchronized (writeLock) {
      roll();
      long eventID = cpi.writerEventID.incrementAndGet();
      SimpleEvent se = new SimpleEvent();
      se.setHeaders(event.getHeaders());
      se.setBody(event.getBody());
      currentWriter.writer(eventID, se);
    }
  }
  
  void gotToNextReader() throws EOFException, IOException {
    
    if (currentReader != null) {
      currentReader.close();
    }
    // currentReader = null;
    int id = cpi.readerFileID.get() + 1;
    
    File rfile = new File(logDir, PREFIX + id);
    if (rfile.exists()) {
      
      LOGGER.info("read form next file:" + rfile);
      currentReader = new LogFilePlus.Reader(rfile, id, 0);
      // cpi.readerFileOffset.set(0);
      cpi.readerFileID.set(id);
    } else {
      LOGGER.info("can't read form next file:" + rfile);
      throw new IOException(
          "can't find the next file, please checkout the code logic");
    }
  }
  
  void restReader(int fileID, long eventID) throws EOFException, IOException {
    
    if (currentReader != null) {
      if (fileID == currentReader.getLogFileID()) {
        // cpi.readerFileOffset.set(offset);
        currentReader.goToID(eventID);
        cpi.readerFileID.set(fileID);
        cpi.readerEventID.set(eventID);
        
      } else {
        currentReader.close();
        File rfile = new File(logDir, PREFIX + fileID);
        if (rfile.exists()) {
          LOGGER.warn("read form next file:" + rfile);
          currentReader = new LogFilePlus.Reader(rfile, fileID, 0);
          cpi.readerFileID.set(fileID);
          currentReader.goToID(eventID);
        } else {
          LOGGER.warn("can't read form next file:" + rfile);
          throw new IOException(
              "can't find the next file, please checkout the code logic");
        }
      }
    }
    
  }
  
  public void flushWriter() throws IOException {
    synchronized (writeLock) {
      currentWriter.flush();
      cpi.writerFlushID.set(cpi.writerEventID.get());
    }
  }
  
  /**
   * Synchronization not required since this method gets the write lock, so
   * checkpoint and this method cannot run at the same time.
   */
  void close() throws IOException {
    if (this.currentReader != null) this.currentReader.close();
    if (this.currentWriter != null) {
      this.flushWriter();
      this.currentWriter.close();
    }
//    shutdownWorker();
    this.cpi.save();
  }
  
  void shutdownWorker() {
    String msg = "Attempting to shutdown background worker.";
    // System.out.println(msg);
    LOGGER.info(msg);
    workerExecutor.shutdown();
    try {
      workerExecutor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted while waiting for worker to die.");
    }
  }
  
  /**
   * Roll a log if needed. Roll always occurs if the log at the index does not
   * exist (typically on startup), or buffer is null. Otherwise
   * LogFile.Writer.isRollRequired is checked again to ensure we don't have
   * threads pile up on this log resulting in multiple successive rolls
   * 
   * Synchronization required since both synchronized and unsynchronized methods
   * call this method, and this method acquires only a read lock. The
   * synchronization guarantees that multiple threads don't roll at the same
   * time.
   * 
   * @param index
   * @throws IOException
   */
  private synchronized void roll() throws IOException {
    
    LogFilePlus.Writer oldLogFile = currentWriter;
    // check to make sure a roll is actually required due to
    // the possibility of multiple writes waiting on lock
    if (oldLogFile == null || oldLogFile.isRollRequired()) {
      try {
        LOGGER.info("Roll start " + logDir);
        int fileID = cpi.writerFileID.incrementAndGet();
        File file = new File(logDir, PREFIX + fileID);
        currentWriter = new LogFilePlus.Writer(file, fileID, rollCount);
        // writer from this point on will get new reference
        if (oldLogFile != null) {
          oldLogFile.close();
        }
      } finally {
        LOGGER.info("Roll end");
      }
    }
  }
  
  private boolean writeCheckpoint() throws Exception {
    return writeCheckpoint(false);
  }
  
  private Boolean writeCheckpoint(Boolean force) throws Exception {
    
    cpi.save();
    // Do the deletes outside the checkpointWriterLock
    // Delete logic is expensive.
    removeOldLogs();
    // Since the exception is not caught, this will not be returned if
    // an exception is thrown from the try.
    return true;
  }
  
  private void removeOldLogs() {
    
    // we will find the smallest fileID currently in use and
    // won't delete any files with an id larger than the min
    int minFileID = currentReader.getLogFileID();
    LOGGER.debug("Files currently in use: " + currentReader.getLogFileID()
        + " - " + currentWriter.getLogFileID());
    List<File> logs = LogUtils.getLogs(logDir);
    // sort oldset to newest
    LogUtils.sort(logs);
    // ensure we always keep two logs per dir
    int size = logs.size() - logFileKeepCount;
    for (int index = 0; index < size; index++) {
      File logFile = logs.get(index);
      int logFileID = LogUtils.getIDForFile(logFile);
      if (logFileID < minFileID) {
        pendingDeletes.add(logFile);
      }
    }
    for (File fileToDelete : pendingDeletes) {
      LOGGER.info("Removing old file: " + fileToDelete);
      FileUtils.deleteQuietly(fileToDelete);
    }
    pendingDeletes.clear();
  }
  
  static class BackgroundWorker implements Runnable {
    private static final Logger LOG = LoggerFactory
        .getLogger(BackgroundWorker.class);
    private final LogPlus log;
    
    public BackgroundWorker(LogPlus log) {
      this.log = log;
    }
    
    @Override
    public void run() {
      try {
        LOG.info(log.cpi.toString());
        log.writeCheckpoint();
      } catch (IOException e) {
        LOG.error("Error doing checkpoint", e);
      } catch (Throwable e) {
        LOG.error("General error in checkpoint worker", e);
      }
    }
  }
  
  public static class CheckpointInfo {
    // public static CheckPointInfo cpi = new CheckPointInfo();
    Properties pro = new Properties();
    File checkpointFile;
    
    private CheckpointInfo() {
      
    }
    
    public static CheckpointInfo get(File checkpointDir)
        throws FileNotFoundException, IOException {
      CheckpointInfo cpi = new CheckpointInfo();
      cpi.checkpointFile = checkpointDir;
      if (checkpointDir.isDirectory()) cpi.checkpointFile = new File(
          checkpointDir, "checkpoint");
      if (cpi.checkpointFile.exists()) {
        FileReader rd = new FileReader(cpi.checkpointFile);
        cpi.pro.load(rd);
        rd.close();
        cpi.readerFileID.set(Integer.parseInt(cpi.pro.getProperty(
            "readerFileID", "0")));
        cpi.readerEventID.set(Long.parseLong(cpi.pro.getProperty(
            "readerEventID", "0")));
        //add auto fix the writeEventId over the readEventId logic
        cpi.writerEventID.set(Math.max(cpi.readerEventID.get(),Long.parseLong(cpi.pro.getProperty(
            "writerEventID", "0"))));
        //cpi.writerEventID.set(Long.parseLong(cpi.pro.getProperty(
          //  "writerEventID", "0")));
        cpi.writerFileID.set(Integer.parseInt(cpi.pro.getProperty(
            "writerFileID", "0")));
        //cpi.writerFlushID.set(Long.parseLong(cpi.pro.getProperty(
          //  "writerFlushID", "0")));
        cpi.writerFlushID.set(Math.max(cpi.writerEventID.get(),Long.parseLong(cpi.pro.getProperty(
            "writerFlushID", "0"))));
        rd.close();
      }
      
      return cpi;
    }
    
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss S");
    
    public void save() throws IOException {
      pro.setProperty("writerEventID", writerEventID.toString());
      pro.setProperty("writerFileID", writerFileID.toString());
      pro.setProperty("readerFileID", readerFileID.toString());
      // pro.setProperty("readerFileOffset", readerFileOffset.toString());
      pro.setProperty("writerFlushID", writerFlushID.toString());
      pro.setProperty("readerEventID", readerEventID.toString());
      FileWriter writer = null;
      try {
        writer = new FileWriter(checkpointFile);
        pro.store(writer, "this checkpoint at:" + df.format(new Date()));
        writer.close();
      } catch (IOException e) {
        if (writer != null) writer.close();
        throw e;
      }
    }
    
    public AtomicLong writerEventID = new AtomicLong(0);
    public AtomicLong writerFlushID = new AtomicLong(0);
    public AtomicInteger writerFileID = new AtomicInteger(0);
    // public long writerFileOffset;
    public AtomicInteger readerFileID = new AtomicInteger(0);
    // public AtomicLong readerFileOffset = new AtomicLong(0);
    public AtomicLong readerEventID = new AtomicLong(0);
    
    @Override
    public String toString() {
      return "CheckpointInfo [checkpointFile=" + checkpointFile
          + ", writerEventID=" + writerEventID + ", writerFlushID="
          + writerFlushID + ", writerFileID=" + writerFileID
          + ", readerFileID=" + readerFileID + ", readerEventID="
          + readerEventID + "]";
    }
  }
}
