package com.maplecloudy.bi.cache;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import com.maplecloudy.bi.util.ReportToDB;

public class TestCacheData {
  @Test
  public void testHashMap() throws IOException, SQLException {
//  System.out.println(ReflectData.get().getSchema(ReportValues.class));
    
    Class<?> cla1 = ReportToDB.class;
    Class<?> cla2 = ReportToDB.class;
    System.out.println(cla1 == cla2);
  }
}
