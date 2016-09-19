
package com.maplecloudy.indexer.solr;

import java.util.Properties;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;

public class CustomCoreDescriptor extends CoreDescriptor {

	Properties coreProperties;
	final CoreContainer coreContainer;

	public CustomCoreDescriptor(CoreContainer coreContainer, String name,
			String instanceDir) {
		super(coreContainer, name, instanceDir);
		this.coreContainer = coreContainer;
	}

	public Properties getCoreProperties() {
		return coreProperties;
	}

	public void setCoreProperties(Properties coreProperties) {
		if (this.coreProperties == null) {
			Properties p = initImplicitProperties();
			this.coreProperties = new Properties(p);
			if (coreProperties != null)
				this.coreProperties.putAll(coreProperties);
		}
	}

	Properties initImplicitProperties() {
		Properties implicitProperties = new Properties(
				coreContainer.getContainerProperties());
		implicitProperties.setProperty("solr.core.name", name);
		implicitProperties.setProperty("solr.core.instanceDir", instanceDir);
		implicitProperties.setProperty("solr.core.dataDir", getDataDir());
		implicitProperties.setProperty("solr.core.configName", configName);
		implicitProperties.setProperty("solr.core.schemaName", schemaName);
		return implicitProperties;
	}

}
