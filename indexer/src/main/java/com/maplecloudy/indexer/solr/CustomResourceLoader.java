package com.maplecloudy.indexer.solr;

import java.io.InputStream;
import java.util.Properties;
import org.apache.solr.core.SolrResourceLoader;
public class CustomResourceLoader extends SolrResourceLoader {

	//private ClassLoader classLoader;
	public CustomResourceLoader(String instanceDir, ClassLoader parent,
			Properties coreProperties) {
		super(instanceDir, parent, coreProperties);
		// this.classLoader = parent;
		// this.classLoader = Thread.currentThread().getContextClassLoader();
		// this.classLoader = URLClassLoader.newInstance(new URL[0],
		// CustomResourceLoader.class.getClassLoader());
		//this.classLoader = ClassLoader.getSystemClassLoader();
		//this.classLoader = CustomResourceLoader.class.getClassLoader();
	}

	@Override
	public InputStream openResource(String resource) {
		InputStream is = null;
		String res = getConfigDir().replaceAll("\\\\", "/") + resource;
//		res = res.substring(1);
		try {
			is = CustomResourceLoader.class.getResourceAsStream(res);
//			URL url = this.getClass().getResource("/");
//			URL nurl = new URL(url,res);
//			System.out.println(nurl);
//			is = nurl.openStream();
			//is = classLoader.getResourceAsStream(res);
			// is = CustomResourceLoader.class.getResourceAsStream(res);
			// is = ClassLoader.getSystemResourceAsStream(res);
		} catch (Exception e) {
			// throw new RuntimeException("Error opening " + res, e);
			e.printStackTrace();
		}

		if (is == null)
			throw new RuntimeException("Can't find resource '" + res
					+ "' in classpath ");
		return is;
	}

	}
