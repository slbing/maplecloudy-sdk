package com.maplecloudy.index.client;

import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.xml.sax.SAXException;

import com.maplecloudy.indexer.solr.CustomCoreContainer;

public abstract class EmbeddedSolrClient<T> extends SolrClient<T> {
  
  CustomCoreContainer container;
  
  @Override
  public void init(Map<String,String> params)
      throws ParserConfigurationException, IOException, SAXException {
    String coreName = params.get("core.name");
    String paths = params.get("data.dir");
    container = new CustomCoreContainer(coreName, paths);
    solrServer = new EmbeddedSolrServer(container, coreName);
  }
 
}
