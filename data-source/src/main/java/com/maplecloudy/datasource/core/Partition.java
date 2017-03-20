package com.maplecloudy.datasource.core;
public class Partition {
  //当前partition对应的分区策略
  PartitionStrategy ps;
  //当前分区的meta信息
  Meta meta;
  //当前分区的存储类型
  SotoreType type;
}
