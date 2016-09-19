package com.maplecloudy.bi.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.avro.AvroTypeException;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.maplecloudy.bi.model.report.ARVNames;
import com.maplecloudy.bi.model.report.RKey;
import com.maplecloudy.bi.model.report.RVNames;
import com.maplecloudy.bi.model.report.ReportKey;
import com.maplecloudy.bi.report.algorithm.ReportAlgorithm;

public class ReportUtils {
  public static TreeSet<String> getReportValueNames(
      Class<? extends ReportKey> keyClass, Configuration conf) {
    Class<?> ra = ReportAlgorithm.getAlgorithm(conf, keyClass);
    
    TreeSet<String> ts = Sets.newTreeSet();
    String[] arr = ra.getAnnotation(RVNames.class).names();
    for (String s : arr) {
      ts.add(s);
    }
    return ts;
  }
  
  public static TreeSet<String> getAllValueNames(
      Class<? extends ReportKey> keyClass, Configuration conf) {
    Class<?> ra = ReportAlgorithm.getAlgorithm(conf, keyClass);
    
    return getAllValueNames(ra);
  }
  
  public static TreeSet<String> getAllValueNames(Class<?> ra) {
    TreeSet<String> ts = Sets.newTreeSet();
    String[] arr = null;
    if (ra.getAnnotation(RVNames.class) != null) {
      arr = ra.getAnnotation(RVNames.class).names();
      for (String s : arr) {
        ts.add(s);
      }
    }
    if (ra.getAnnotation(ARVNames.class) != null) {
      arr = ra.getAnnotation(ARVNames.class).activelyNames();
      for (String s : arr) {
        ts.add(s);
      }
    }
    return ts;
  }
  
  public static TreeSet<String> getKeyNames(Class<?> ra) {
    Class<? extends ReportKey> rk = ra.getAnnotation(RKey.class).reportKey();
    return getFields(rk);
  }
  
  @SuppressWarnings("rawtypes")
  private static TreeSet<String> getFields(Class recordClass) {
    Map<String,Field> fields = new LinkedHashMap<String,Field>();
    Class c = recordClass;
    do {
      if (c.getPackage() != null
          && c.getPackage().getName().startsWith("java.")) break; // skip java
                                                                  // built-in
                                                                  // classes
      for (Field field : c.getDeclaredFields())
        if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0) if (fields
            .put(field.getName(), field) != null) throw new AvroTypeException(c
            + " contains two fields named: " + field);
      c = c.getSuperclass();
    } while (c != null);
    TreeSet<String> ts = new TreeSet<String>();
    ts.addAll(fields.keySet());
    return ts;
  }
  
  public static TreeMap<String,List<Integer>> getActivelys(
      Class<? extends ReportKey> keyClass, Configuration conf) {
    Class<?> ra = ReportAlgorithm.getAlgorithm(conf, keyClass);
    TreeMap<String,List<Integer>> ret = Maps.newTreeMap();
    if (ra.getAnnotation(ARVNames.class) == null) return ret;
    String[] names = ra.getAnnotation(ARVNames.class).activelyNames();
    
    for (String name : names) {
      String[] arr = name.split("-");
      String tname;
      Integer actively;
      if (arr.length > 1) {
        tname = arr[0];
        actively = Integer.parseInt(arr[1]);
      } else {
        tname = arr[0];
        actively = 0;
      }
      if (ret.get(tname) != null) ret.get(tname).add(actively);
      else {
        List<Integer> lst = Lists.newArrayList();
        lst.add(actively);
        ret.put(tname, lst);
      }
    }
    return ret;
  }
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static List<Class<? extends ReportAlgorithm>> getAlgorithmClasses(
      String[] algorithms) throws ClassNotFoundException {
    List<Class<? extends ReportAlgorithm>> ras = Lists.newArrayList();
    for (String algorithm : algorithms) {
      ras.add((Class<? extends ReportAlgorithm>) Class.forName(algorithm));
    }
    
    return ras;
  }
  
  @SuppressWarnings({"rawtypes"})
  public static List<String> getAlgorithmString(
          List<Class<? extends ReportAlgorithm>> ras) throws ClassNotFoundException {
    List<String> algorithms = Lists.newArrayList();
    for (Class<? extends ReportAlgorithm> ra : ras) {
        algorithms.add(ra.getClass().getSimpleName());
    }
    
    return algorithms;
  }
}
