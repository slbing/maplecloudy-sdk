package com.maplecloudy.flume.channel;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.maplecloudy.avro.io.MapAvroFile;
import com.maplecloudy.avro.io.Pair;
import com.maplecloudy.avro.reflect.ReflectDataEx;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LogFilePlus {
  
  private static final Logger LOG = LoggerFactory.getLogger(LogFilePlus.class);
  
  public static class Reader {
    
    private File file;
    
    private int logFileID;
    
    private MapAvroFile.Reader<Long,Event> reader;
    
    /**
     * Construct a Sequential Log Reader object
     * 
     * @param file
     * @throws IOException
     *           if an I/O error occurs
     * @throws EOFException
     *           if the file is empty
     */
    Reader(File file, int logFileID, long eventID) throws IOException,
        EOFException {
      this.setFile(file);
      this.logFileID = logFileID;
      this.reader = new MapAvroFile.Reader<Long,Event>(file);
      this.goToID(eventID);
    }
    
    protected void setLogFileID(int logFileID) {
      this.logFileID = logFileID;
      Preconditions.checkArgument(logFileID >= 0, "LogFileID is not positive: "
          + Integer.toHexString(logFileID));
      
    }
    
    int getLogFileID() {
      return logFileID;
    }
    
    public void close() {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {}
      }
    }
    
    public void goToID(long eventID) throws IOException {
      if (this.reader.get(eventID) == null) {
//          System.out.println("*********************************");
        reader.reset();
      }
      
    }
    
    public File getFile() {
      return file;
    }
    
    public void setFile(File file) {
      this.file = file;
    }
    
    public boolean hasNext(long eventID) throws IOException {
      
      if(!this.reader.hasNext())
      {
        reader.reLoadIndex();
        reader.get(eventID);
        return reader.hasNext();
      }
      else
      {
        return true;
      }
      
    }
    
    public Pair<Long,Event> next() throws IOException {
      return this.reader.next();
    }
  }
  
  public static class Writer {
    private final int logFileID;
    MapAvroFile.Writer<Long,SimpleEvent> writer;
    private final File file;
    private volatile boolean open;
    
    private long writeCount = 0;
    // To ensure we can count the number of fsyncs.
    private long syncCount = 0;
    
    public int rollCount = 1000000;
    
    public Writer(File file, int logFileID, int rollCount) throws IOException {
      
      this.file = file;
      this.logFileID = logFileID;
      
      writer = new MapAvroFile.Writer<Long,SimpleEvent>(
          file, Schema.create(Schema.Type.LONG), ReflectDataEx.get()
              .getSchema(SimpleEvent.class));
      LOG.info("Opened " + file);
      this.rollCount = rollCount;
      open = true;
    }
    
    int getLogFileID() {
      return logFileID;
    }
    
    File getFile() {
      return file;
    }
    
    String getParent() {
      return file.getParent();
    }
    
    long getSyncCount() {
      return syncCount;
    }
    
    public void writer(Long key, SimpleEvent value) throws IOException {
      writeCount++;
      writer.append(key, value);
    }
    
    public void flush() throws IOException {
      syncCount = writeCount;
      writer.flush();
    }
    
    protected boolean isOpen() {
      return open;
    }
    
    synchronized void close() {
      if (open) {
        open = false;
        LOG.info("Closing " + file);
        if (writer != null) {
          try {
            writer.flush();
            writer.close();
          } catch (IOException e) {
            LOG.warn("Unable to close " + file, e);
          }
        }
      }
    }
    
    public boolean isRollRequired() {
      return this.writeCount >= rollCount;
    }
  }
  
}
