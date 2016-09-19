package com.maplecloudy.indexer;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputDocument;

import com.maplecloudy.index.util.StringUtil;

/**
 * Class that performs SOLR indexing done on a hadoop cluster.
 * 
 * @author dan.brown
 */
public abstract class HadoopIndexerBase {
  public static final Log logger = LogFactory.getLog(HadoopIndexerBase.class);
  
  /**
   * Add a value to the field specified to the document. This calls the other
   * addSolrField method passing false as the last argument
   * 
   * @param solrDoc
   * @param name
   * @param value
   */
  public static void addSolrField(final SolrInputDocument solrDoc,
      final String name, final Object value) {
    addSolrField(solrDoc, name, value, false);
  }
  
  /**
   * Add a value to the field specified to the document
   * 
   * @param solrDoc
   * @param name
   * @param value
   * @param convertToString
   *          should the value be converted to a string before getting added
   */
  public static void addSolrField(final SolrInputDocument solrDoc,
      final String name, Object value, final boolean convertToString) {
    if ((solrDoc == null) || (name == null) || (value == null)) {
      return;
    }
    
    if (convertToString) {
      value = value.toString();
    }
    
    solrDoc.addField(name, value);
  }
  
  /**
   * A method used for generating time values that can be used for reporting
   * 
   * @param time0
   * @param time1
   * @return list of: hours, minutes, seconds
   */
  private long[] getTimeValues(final long time0, final long time1) {
    final long totalSeconds = (int) ((time1 - time0) / 1000);
    final long totalMinutes = totalSeconds / 60;
    final long hours = totalMinutes / 60;
    final long minutes = totalMinutes % 60;
    final long seconds = totalSeconds % 60;
    
    final long[] timeValues = {hours, minutes, seconds};
    
    return timeValues;
  }
  
  /**
   * Add the value only if it is not empty
   * 
   * @param solrDoc
   * @param fieldName
   * @param value
   * @param convertToString
   *          should the value be converted to a string before being added
   */
  public static void addDocumentIfNotEmpty(final SolrInputDocument solrDoc,
      final String fieldName, final String value, final boolean convertToString) {
    if (!StringUtil.isNullOrEmpty(value)) {
      addSolrField(solrDoc, fieldName, value, convertToString);
    }
  }
  
  /**
   * Add the value only if it is not empty
   * 
   * @param solrDoc
   * @param fieldName
   * @param value
   * @param convertToString
   *          should the value be converted to a string before being added
   */
  public static void addDocumentIfNotEmpty(final SolrInputDocument solrDoc,
      final String fieldName, final Date value, final boolean convertToString) {
    if (value != null) {
      addSolrField(solrDoc, fieldName, value, convertToString);
    }
  }
  
  public static void addDocumentIfNotEmpty(final SolrInputDocument solrDoc,
      final String fieldName, final Object value, final boolean convertToString) {
    if (value != null) {
      addSolrField(solrDoc, fieldName, value, convertToString);
    }
  }
  
  /**
   * Add the value only if it is not empty
   * 
   * @param solrDoc
   * @param fieldName
   * @param value
   * @param convertToString
   *          should the value be converted to a string before being added
   */
  public static void addDocumentIfNotEmpty(final SolrInputDocument solrDoc,
      final String fieldName, final Double value, final boolean convertToString) {
    if (value != null) {
      addSolrField(solrDoc, fieldName, value, convertToString);
    }
  }
  
  /**
   * Add the value only if it is not empty
   * 
   * @param solrDoc
   * @param fieldName
   * @param value
   * @param convertToString
   *          should the value be converted to a string before being added
   */
  protected void addDocumentIfNotEmpty(final SolrInputDocument solrDoc,
      final String fieldName, final Float value, final boolean convertToString) {
    if (value != null) {
      addSolrField(solrDoc, fieldName, value, convertToString);
    }
  }
  
}
