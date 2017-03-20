package com.maplecloudy.bi.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigProperties extends Properties {
  /**
   * 
   */
  private static final long serialVersionUID = -784884919826163168L;
  public static final String BOOST_FIELD_SEPARATOR = ",";
  public static final String NAME_VALUE_SEPARATOR = ":";
  
  /**
   * Protected so that it can only be instantiated by the {@link ConfigFactory}
   */
  protected ConfigProperties() {
    super();
  }
  
  /**
   * Constructor that uses the {@link Properties} provided. Protected so that it
   * can only be instantiated by the {@link ConfigFactory}
   * 
   * @param defaults
   *          properties to use
   * @see Properties
   */
  protected ConfigProperties(Properties defaults) {
    super(defaults);
  }
  
  /**
   * Constructor that loads properties from the file given. Protected so that it
   * can only be instantiated by the {@link ConfigFactory}
   * 
   * @param filePath
   *          is the class path to the config file
   */
  public ConfigProperties(final String filePath) {
    final InputStream inStream = ConfigProperties.class
        .getResourceAsStream(filePath);
    try {
      load(inStream);
    } catch (final Exception e) {
      throw new NullPointerException("Failed to load config file: " + filePath
          + ", error: " + e.getMessage());
    } finally {
      if (inStream != null) {
        try {
          inStream.close();
        } catch (final IOException e) {
          // do nothing
        }
      }
      
    }
  }
  
  /**
   * Get a int value. The return value from
   * {@link Properties#getProperty(String)} is converted to a int. Returns
   * default value if property name does not exist.
   * 
   * @param propertyName
   *          name of the property
   * @param defaultValue
   *          if property name does not exist return this value
   * @return value corresponding to the name
   * @see Properties#getProperty(String)
   */
  public int getInt(final String propertyName, final int defaultValue) {
    int propertyValue = defaultValue;
    
    final String valueStr = getProperty(propertyName);
    try {
      propertyValue = Integer.parseInt(valueStr);
    } catch (final Exception e) {
      // do nothing, just return the default value;
    }
    
    return propertyValue;
  }
  
  /**
   * Get a float value. The return value from
   * {@link Properties#getProperty(String)} is converted to a float. Returns
   * default value if property name does not exist.
   * 
   * @param propertyName
   *          name of the property
   * @param defaultValue
   *          if property name does not exist return this value
   * @return value corresponding to the name
   * @see Properties#getProperty(String)
   */
  public float getFloat(final String propertyName, final float defaultValue) {
    float propertyValue = defaultValue;
    
    final String valueStr = getProperty(propertyName);
    try {
      propertyValue = Float.parseFloat(valueStr);
    } catch (final Exception e) {
      // do nothing, just return the default value;
    }
    
    return propertyValue;
  }
  
  /**
   * Get a double value. The return value from
   * {@link Properties#getProperty(String)} is converted to a double. Returns
   * default value if property name does not exist.
   * 
   * @param propertyName
   *          name of the property
   * @param defaultValue
   *          if property name does not exist return this value
   * @return value corresponding to the name
   * @see Properties#getProperty(String)
   */
  public double getDouble(final String propertyName, final double defaultValue) {
    double propertyValue = defaultValue;
    
    final String valueStr = getProperty(propertyName);
    try {
      propertyValue = Double.parseDouble(valueStr);
    } catch (final Exception e) {
      // do nothing, just return the default value;
    }
    
    return propertyValue;
  }
  
  /**
   * Get a boolean value. The return value from
   * {@link Properties#getProperty(String)} is converted to a boolean. Returns
   * default value if property name does not exist.
   * 
   * @param propertyName
   *          name of the property
   * @param defaultValue
   *          if property name does not exist return this value
   * @return value corresponding to the name
   * @see Properties#getProperty(String)
   */
  public boolean getBoolean(final String propertyName,
      final boolean defaultValue) {
    boolean propertyValue = defaultValue;
    
    final String valueStr = getProperty(propertyName);
    try {
      propertyValue = Boolean.parseBoolean(valueStr);
    } catch (final Exception e) {
      // do nothing, just return the default value;
    }
    
    return propertyValue;
  }
  
  /**
   * Get a string value. This is the same as
   * {@link Properties#getProperty(String)} but conforms to the format of the
   * other method names found in this class. Returns null if property name does
   * not exist.
   * 
   * @param propertyName
   *          name of the property
   * @return value corresponding to the name
   * @see Properties#getProperty(String)
   */
  public String getString(final String propertyName) {
    return getProperty(propertyName);
  }
  
  /**
   * Get a string value. This is the same as
   * {@link Properties#getProperty(String, String)} but conforms to the format
   * of the other method names found in this class.
   * 
   * @param propertyName
   *          name of the property
   * @return value corresponding to the name
   * @see Properties#getProperty(String, String)
   */
  public String getString(final String propertyName, final String defaultValue) {
    return getProperty(propertyName, defaultValue);
  }
  
  /**
   * Get a long value. The return value from
   * {@link Properties#getProperty(String)} is converted to a long. Returns
   * default value if property name does not exist.
   * 
   * @param propertyName
   *          name of the property
   * @param defaultValue
   *          if property name does not exist return this value
   * @return value corresponding to the name
   * @see Properties#getProperty(String)
   */
  public long getLong(final String propertyName, final long defaultValue) {
    long propertyValue = defaultValue;
    
    final String valueStr = getProperty(propertyName);
    try {
      propertyValue = Long.parseLong(valueStr);
    } catch (final Exception e) {
      // do nothing, just return the default value;
    }
    
    return propertyValue;
  }
  
  public String getLogPrefix() {
    return getProperty("log_prefix", ": ");
  }
}
