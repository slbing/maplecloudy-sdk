package com.maplecloudy.indexer.solr;

import java.util.ArrayList;
import java.util.List;

/**
 * solr constatns
 * 
 * @author CC
 * 
 */
public class DynamicIndexConstant {
  public static final String INT = "*_i";
  public static final String LONG = "*_l";
  public static final String FLOAT = "*_f";
  public static final String BOOLEAN = "*_b";
  public static final String DOUBLE = "*_d";
  public static final String STRING = "*_s";
  public static final String DATE = "*_date";
  public static final String TEXT_GENERAL = "*_t";
  public static final String MINT = "*_mi";
  public static final String MLONG = "*_ml";
  public static final String MFLOAT = "*_mf";
  public static final String MBOOLEAN = "*_mb";
  public static final String MDOUBLE = "*_md";
  public static final String MSTRING = "*_ms";
  public static final String MDATE = "*_mdate";
  public static final String MTEXT_GENERAL = "*_mt";
  
  public static final String TINT = "*_ti";
  public static final String TLONG = "*_tl";
  public static final String TFLOAT = "*_tf";
  public static final String TDOUBLE = "*_td";
  
  public static final String MTINT = "*_mti";
  public static final String MTLONG = "*_mtl";
  public static final String MTFLOAT = "*_mtf";
  public static final String MTDOUBLE = "*_mtd";
  
  
  
  public final static Byte BYTE_INVALID_VALUE = Byte.MIN_VALUE;
  public final static Long LONG_INVALID_VALUE = Long.MIN_VALUE;
  public final static Integer INTEGER_INVALID_VALUE = Integer.MIN_VALUE;
  public final static Double DOUBLE_INVALID_VALUE = Double.MIN_VALUE;
  public final static Float FLOAT_INVALID_VALUE = Float.MIN_VALUE;
  
  public static List<Object> InvalidValue = new ArrayList<Object>();
  static {
    InvalidValue.add(BYTE_INVALID_VALUE);
    InvalidValue.add(LONG_INVALID_VALUE);
    InvalidValue.add(INTEGER_INVALID_VALUE);
    InvalidValue.add(DOUBLE_INVALID_VALUE);
    InvalidValue.add(FLOAT_INVALID_VALUE);
  }
}
