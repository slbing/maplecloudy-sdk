package com.maplecloudy.datasource.core;

import java.util.List;

public class DataSource {
  public ShcemaIndefine si;
  //记录这个DataSource使用过的所有partition策略，可以用于通过策略扫描构建整个DataSource
  public List<PartitionStrategy> pss;
  
  public List<Partition> partitions;
}
