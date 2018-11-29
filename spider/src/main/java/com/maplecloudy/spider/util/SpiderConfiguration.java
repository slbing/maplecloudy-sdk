/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.maplecloudy.spider.util;

// JDK imports
import java.util.Enumeration;

import javax.servlet.ServletContext;

import org.apache.hadoop.conf.Configuration;

/**
 * Utility to create Hadoop {@link Configuration}s that include Spider-specific
 * resources.
 */
public class SpiderConfiguration {

	private final static String KEY = SpiderConfiguration.class.getName();

	private SpiderConfiguration() {
	} // singleton

	
	/** Create a {@link Configuration} for Spider. */
	public static Configuration create() {
		Configuration conf = new Configuration();
		addSpiderResources(conf);
		return conf;
	}
	
	/** Create a {@link Configuration} for Spider. */
  public static Configuration create(Configuration confo) {
    Configuration conf = new Configuration(confo);
    addSpiderResources(conf);
    return conf;
  }

	/**
	 * Create a {@link Configuration for Spider invoked with the command line
	 * crawl command, i.e. bin/Spider crawl ...
	 */
	public static Configuration createCrawlConfiguration() {
		Configuration conf = new Configuration();
		addSpiderResources(conf);
		return conf;
	}

	/**
	 * Create a {@link Configuration} for Spider front-end.
	 * 
	 * If a {@link Configuration} is found in the
	 * {@link javax.servlet.ServletContext} it is simply returned, otherwise, a
	 * new {@link Configuration} is created using the {@link #create()} method,
	 * and then all the init parameters found in the
	 * {@link javax.servlet.ServletContext} are added to the
	 * {@link Configuration} (the created {@link Configuration} is then saved
	 * into the {@link javax.servlet.ServletContext}).
	 * 
	 * @param application
	 *            is the ServletContext whose init parameters must override
	 *            those of Spider.
	 */
	public static Configuration get(ServletContext application) {
		Configuration conf = (Configuration) application.getAttribute(KEY);
		if (conf == null) {
			conf = create();
			Enumeration<?> e = application.getInitParameterNames();
			while (e.hasMoreElements()) {
				String name = (String) e.nextElement();
				conf.set(name, application.getInitParameter(name));
			}
			application.setAttribute(KEY, conf);
		}
		return conf;
	}

	/**
	 * Add the standard Spider resources to {@link Configuration}.
	 * 
	 * @param conf
	 *            Configuration object to which configuration is to be added.
	 * @param crawlConfiguration
	 *            Whether configuration for command line crawl using 'bin/Spider
	 *            crawl' command should be added.
	 */
	private static Configuration addSpiderResources(Configuration conf) {
		conf.addResource("spider.xml");
		return conf;
	}
}
