package com.maplecloudy.geoip.constant;

import com.maplecloudy.geoip.model.LocalId;

/**
 * @author cc
 * @creation date 2012-7-26 下午04:18:13
 * @Description
 * @version 1.0
 */
public class Constants {
  /**
   * 默认的中国地区编码，同省一级别，目前lm找不到的ip都落到这里
   */
  public static final LocalId DEFAULT_AREA_ID = new LocalId("8690");
  public static final int INVALID_ID = -1;
  
  public static final String GEO_STORE_ROOT = "/user/sunflower/knowledge-db";
  public static final String GEO_STORE_SUB_TXT = "txt/record.txt";
  public static final String GEO_STORE_SUB_AVRO = "avro";
}