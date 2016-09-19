package com.maplecloudy.bi.model;

import java.io.IOException;
import java.util.Date;

import org.junit.Test;

import com.maplecloudy.bi.ReportConstants;

public class TestSystemProperty {
  @Test
  public void testReportKey() throws IOException {
    System.out.println(ReportConstants.FORMAT_OOZIE
        .format(new Date(1352559601345L)));
  }
}
