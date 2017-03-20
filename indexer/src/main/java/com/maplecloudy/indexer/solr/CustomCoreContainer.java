package com.maplecloudy.indexer.solr;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

public class CustomCoreContainer extends CoreContainer {
  protected static Logger log = LoggerFactory
      .getLogger(CustomCoreContainer.class);
  
  public CustomCoreContainer() {
    solrHome = "/solr/";
  }
  
  public CustomCoreContainer(String coreName, String dataDir)
      throws ParserConfigurationException, IOException, SAXException {
    this(coreName, coreName, dataDir);
  }
  
  public CustomCoreContainer(String coreName, String instanceDir, String dataDir)
      throws ParserConfigurationException, IOException, SAXException {
    solrHome = "/solr/";
    CustomCoreDescriptor cd = new CustomCoreDescriptor(this, coreName,
        instanceDir);
    // cd.setDataDir(dataDir);
    SolrCore solrCore = this.create(cd, dataDir);
    this.register(solrCore, false);
  }
  
  public void addCore(String coreName, String instanceDir, String dataDir)
      throws ParserConfigurationException, IOException, SAXException {
    CustomCoreDescriptor cd = new CustomCoreDescriptor(this, coreName,
        instanceDir);
    SolrCore solrCore = this.create(cd, dataDir);
    this.register(solrCore, false);
  }
  
  public SolrCore create(CustomCoreDescriptor dcore, String dataDir)
      throws ParserConfigurationException, IOException, SAXException {
    // Make the instanceDir relative to the cores instanceDir if not
    // absolute
    File idir = new File(dcore.getInstanceDir());
//    if (!idir.isAbsolute()) {
//      idir = new File(solrHome, dcore.getInstanceDir());
//    }
    String instanceDir = idir.getPath();
    
    // Initialize the solr config
    CustomResourceLoader solrLoader = new CustomResourceLoader(instanceDir,
        libLoader, getCoreProps(instanceDir, dcore.getPropertiesName(),
            this.containerProperties));
    SolrConfig config = new SolrConfig(solrLoader, dcore.getConfigName(), null);
    IndexSchema schema = null;
    if (indexSchemaCache != null) {
      // schema sharing is enabled. so check if it already is loaded
      File schemaFile = new File(dcore.getSchemaName());
      if (!schemaFile.isAbsolute()) {
        schemaFile = new File(solrLoader.getInstanceDir() + "conf"
            + File.separator + dcore.getSchemaName());
      }
      if (schemaFile.exists()) {
        String key = schemaFile.getAbsolutePath()
            + ":"
            + new SimpleDateFormat("yyyyMMddhhmmss").format(new Date(schemaFile
                .lastModified()));
        schema = indexSchemaCache.get(key);
        if (schema == null) {
          log.info("creating new schema object for core: " + dcore.getName());
          schema = new IndexSchema(config, dcore.getSchemaName(), null);
          indexSchemaCache.put(key, schema);
        } else {
          log.info("re-using schema object for core: " + dcore.getName());
        }
      }
    }
    if (schema == null) {
      schema = new IndexSchema(config, dcore.getSchemaName(), null);
    }
    SolrCore core = new SolrCore(dcore.getName(), dataDir, config, schema,
        dcore);
    return core;
  }
  
  private static Properties getCoreProps(String instanceDir, String file,
      Properties defaults) {
    if (file == null) file = "conf" + File.separator + "solrcore.properties";
    File corePropsFile = new File(file);
    if (!corePropsFile.isAbsolute()) {
      corePropsFile = new File(instanceDir, file);
    }
    Properties p = defaults;
    if (corePropsFile.exists() && corePropsFile.isFile()) {
      p = new Properties(defaults);
      InputStream is = null;
      try {
        is = new FileInputStream(corePropsFile);
        p.load(is);
      } catch (IOException e) {
        log.warn("Error loading properties ", e);
      } finally {
        IOUtils.closeQuietly(is);
      }
    }
    return p;
  }
}
