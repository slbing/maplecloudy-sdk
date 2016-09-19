package com.maplecloudy.bi.main.scheduler;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.maplecloudy.bi.ReportConstants;
import com.maplecloudy.bi.ReportFrequence;
import com.maplecloudy.bi.util.ReportConfiguration;
import com.maplecloudy.bi.util.ReportConstantsUtils;

public class TestSchedulerReportMain implements ReportConstants {
  @Test
  public void testCreateSqoopFlow() throws IOException, SQLException,
      ParseException {
//    SchedulerReportMain srm = new SchedulerReportMain();
//    srm.setConf(ReportConfiguration.create());
//    Date startDate = FORMAT_OOZIE.parse("2012-10-25T08:00Z");
//    
//    ReportFrequence frequence = ReportFrequence.valueOf("Hourly");
//    Date endDate = ConstantsUtils.getEdndate(startDate, frequence);
//    List<Path> inputs = ConstantsUtils.getLogInput(startDate, endDate);
//    Path output = ConstantsUtils.getReportOutput(startDate, frequence);
//    String[] algorithms = ConstantsUtils.getAlgorithms(frequence);
//    srm.getConf().setStrings(REPORT_ALGORITHMS, algorithms);
//    srm.getConf().setInt(REPORT_START_TIME, (int) (startDate.getTime() / 1000));
//    srm.getConf().set(REPORT_FREQUENCE, "Hourly");
//    GenericReportMain grm = new GenericReportMain();
//    grm.setConf(srm.getConf());
//    System.out.println(grm.createSqoopFlow(output));
  }
  
  @Test
  public void testDate() throws IOException, SQLException, ParseException {
    
    Date startDate = FORMAT_OOZIE.parse("2012-10-25T08:00Z");
    
    // ReportFrequence frequence = ReportFrequence.valueOf("Hourly");
    ReportFrequence frequence = ReportFrequence.valueOf("Hourly");
    Date endDate = ReportConstantsUtils.getEdndate(startDate, frequence);
    System.out.println(ReportConstants.FORMAT_OOZIE.format(startDate));
    System.out.println(ReportConstants.FORMAT_OOZIE.format(endDate));
  }
 
}
