package com.maplecloudy.source.report;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;

public interface DateSource extends Source {
  public List<Path> getInputs(Date startDate, Date endDate);  
  
  public Path getOutput(Date date);
}