package com.maplecloudy.bi.model.report;


public interface ActivelyReportKey<RK extends ReportKey> extends ReportKey {
  public RK getSuperKey();
}
