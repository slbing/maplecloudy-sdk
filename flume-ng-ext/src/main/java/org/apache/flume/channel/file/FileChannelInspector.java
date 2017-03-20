package org.apache.flume.channel.file;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flume.Context;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.maplecloudy.avro.util.AvroUtils;
import com.maplecloudy.flume.sink.AvroFileSerializer;
import com.maplecloudy.flume.sink.LogSchemaSource;

public class FileChannelInspector {
  
  /**
   * @param args
   * @throws IOException
   * @throws EOFException
   */
  LogFileV3.SequentialReader reader;
  long currentPos;
  boolean isFollow;
  boolean tail;
  
  public FileChannelInspector(File dataFile, boolean isFollow)
      throws EOFException, IOException {
    this.currentPos = 0;
    this.reader = new LogFileV3.SequentialReader(dataFile, null,true);
    if (isFollow) {
      // skip to MAX_VALUE and store the actual position of last CheckPoint
      reader.skipToLastCheckpointPosition(Long.MAX_VALUE);
      storePosition();
    }
    
  }
  
  private void storePosition() throws IOException {
    currentPos = reader.getPosition();
    reader.setLastCheckpointPosition(currentPos);
    reader.setLastCheckpointWriteOrderID(currentPos);
  }
  
  private void backToLastValidPos() throws IOException {
    reader.skipToLastCheckpointPosition(currentPos);
  }
  
  public LogRecord next() throws CorruptEventException, IOException,
      InterruptedException {
    LogRecord record = null;
    
    while (record == null) {
      record = reader.next();
      if (record != null) {
        storePosition();
        break;
      } else {
        if (isFollow) {
          backToLastValidPos();
          Thread.sleep(500);
        }
      }
    }
    return record;
  }
  
  public void inspect(LogRecord record) throws IOException {
    StringBuilder msg = new StringBuilder();
    TransactionEventRecord event = record.getEvent();
    boolean isPut = event instanceof Put;
    if (!isPut) {
      return;
    }
    Put put = (Put) event;
    msg.append(put.getTransactionID());
    msg.append("|||\t");
    FlumeEvent flumeEvent = put.getEvent();
    String sn = flumeEvent.getHeaders().get("s.n");
    String sv = flumeEvent.getHeaders().get("s.v");
    AvroFileSerializer serializer = LogSchemaSource.getInstance(new Context())
        .getAvroSerializer(sn, sv);
    String log = null;
    log = AvroUtils.toAvroString(serializer.deSerialize(flumeEvent));
    msg.append(log);
    System.out.println(msg.toString());
  }
  
  private static void inspectFile(File dataFile, boolean isFollow,
      int maxInspect) throws EOFException, IOException {
    
    long currentPos = 0;
    int recordCount = 0;
    FileChannelInspector inspector = new FileChannelInspector(dataFile,
        isFollow);
    while (true) {
      try {
        LogRecord record = inspector.next();
        if (record != null) {
          recordCount++;
          inspector.inspect(record);
          if (!isFollow && recordCount >= maxInspect) {
            System.out.println("Read " + recordCount + " record");
            break;
          }
        } else {
          break;
        }
      } catch (CorruptEventException e) {
        System.err.println("Corruption found in " + dataFile.toString()
            + " at " + currentPos);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  public static void main(String[] args) throws EOFException, IOException,
      ParseException {
    Logger logger = Logger.getRootLogger();
    logger.setLevel(Level.FATAL);
    
    Options options = new Options();
    
    Option option = new Option("l", "log-file", true, "log file path of Flume");
    options.addOption(option);
    
    option = new Option(
        "f",
        "follow",
        false,
        "whether to read from the end and follow the file, otherwise will read from start of file");
    options.addOption(option);
    
    option = new Option("m", "max-records", true,
        "max number of records to display, only in non-follow mode");
    options.addOption(option);
    
    CommandLineParser parser = new GnuParser();
    CommandLine commandLine = parser.parse(options, args);
    String max = commandLine.getOptionValue('m');
    String path = commandLine.getOptionValue("l");
    boolean isFollow = commandLine.hasOption("f");
    
    if (!commandLine.hasOption("l") || path == null) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("-l <log-file> [-m max-records] [-f]", options);
      return;
    }
    
    if (!new File(path).exists()) {
      System.out.println("log file not exists: " + path);
      return;
    }
    
    int maxInspect = 0;
    if (max != null) {
      maxInspect = Integer.parseInt(max);
    }
    
    inspectFile(new File(path), isFollow, maxInspect);
  }
}
