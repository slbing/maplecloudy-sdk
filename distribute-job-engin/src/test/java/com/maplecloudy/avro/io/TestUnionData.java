package com.maplecloudy.avro.io;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.maplecloudy.distribute.engine.UnionData;

public class TestUnionData {
  @Test
  public void testScheam() {
    Configuration conf = new Configuration();
    
    UnionData.setUnionClass(conf, String.class, Integer.class);
    UnionData ud = new UnionData();
    ud.setConf(conf);
    System.out.println(ud.schema);
    ud.datum = "4531453";
    System.out.println(ud.hashCode());
  }
}
