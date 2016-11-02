package com.maplecloudy.indexer.solr;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * @author lbsun
 *  (*)标示当前字段的属性名
 *  (*_mi)代表动态多值索引
 *  (*aaa)代表当前字段名字+aaa作为索引名字
 */
 
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER,ElementType.FIELD})
@Documented
public @interface DynamicIndex {
  String value();
}
