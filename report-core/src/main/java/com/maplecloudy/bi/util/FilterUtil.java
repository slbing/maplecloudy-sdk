package com.maplecloudy.bi.util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

public class FilterUtil {

	static public final String FILTER_SEG = ";";
	static public final String FILTER_NAME_NORMAL = "normal";
	static public final String FILTER_NAME_PERF = "perf";
	static public final String FILTER_NAME_ERROR = "err";
	static public final String[] FILTER_NAME_LIST = {FILTER_NAME_NORMAL,FILTER_NAME_PERF,FILTER_NAME_ERROR};
	
	static public final String FILTER_APPS = "filter.app.";	
	static public final String FILTER_COMPS = "filter.comp.";
	static public final String FILTER_LAYERS = "filter.layer.";
	
	static public final String OTHER = "other";

	private Configuration conf = null;

	private static Map<String,Set<String>> filter_apps = new HashMap<String, Set<String>>();
	private static Map<String,Set<String>> filter_comps = new HashMap<String, Set<String>>();
	private static Map<String,Set<String>> filter_layers = new HashMap<String, Set<String>>();
	
	private static Map<String,Map<String,Set<String>>> filter_all = null;

	private static FilterUtil filterUtil = null;
	
	public static void set(Configuration c){
		filterUtil = new FilterUtil(c);
	}
	
	public static FilterUtil get(){
		return filterUtil;
	}

	public FilterUtil(Configuration c) {
		conf = c;
		filter_all = new HashMap<String, Map<String,Set<String>>>();
		filter_all.put(FILTER_APPS, filter_apps);
		filter_all.put(FILTER_COMPS, filter_comps);
		filter_all.put(FILTER_LAYERS, filter_layers);
	}

	/**
	 * 从数据库中获取需要过滤的appid,用';'分隔.
	 * @throws IOException 
	 */
	public void getFilterFromDb() throws ClassNotFoundException, SQLException, IOException{
		FilterDbOp fdbop = new FilterDbOp(conf);
		for (String filter_name : FILTER_NAME_LIST){
			String ret = fdbop.getFilterApps(filter_name);
			if (null!=ret && ret.length()>0)
				conf.set(FILTER_APPS+filter_name, ret);
			
			ret = fdbop.getFilterComps(filter_name);
			if (null!=ret && ret.length()>0)
				conf.set(FILTER_COMPS+filter_name, ret);
			
			ret = fdbop.getFilterLayers(filter_name);
			if (null!=ret && ret.length()>0)
				conf.set(FILTER_LAYERS+filter_name, ret);
		}
		fdbop.closeConn();
	}

	/**
	 * 从configuration中获取需要过滤的appid,并转化成集合存储下来
	 */
	public void setFilter(){
		for (String filter_name : FILTER_NAME_LIST){
			setFilter(FILTER_APPS,filter_name);
			setFilter(FILTER_COMPS,filter_name);
			setFilter(FILTER_LAYERS,filter_name);
		}
	}
	
	public void setFilter(String filter_type, String filter_name){
		Map<String,Set<String>> filter = filter_all.get(filter_type);
		if (null == filter){
			filter = new HashMap<String, Set<String>>();
			filter_all.put(filter_type, filter);
		}
		
		String val = conf.get(filter_type+filter_name);
		if (null != val){
			String[] values = val.split(FILTER_SEG);
			if (null!=values && values.length>0){
				Set<String> vSet = new HashSet<String>();
				for (String str : values){
					vSet.add(str.toLowerCase());
				}
				filter.put(filter_name, vSet);
			}
		}
	}

	public String getFilterApp(String app_id){
		return getFilterApp(app_id,FILTER_NAME_NORMAL);
	}
	
	public String getFilterApp(String app_id, String filter_name){
		if (null == app_id || 
				(null != filter_apps && filter_apps.containsKey(filter_name) 
				&& filter_apps.get(filter_name).contains(app_id.toLowerCase()))){
			return app_id;
		}else{
			return OTHER;
		}
	}
	
	public boolean isFilterComp(String comp,String filter_name){
		if (null != filter_comps && filter_comps.containsKey(filter_name)
				&& filter_comps.get(filter_name).contains(comp.toLowerCase())){
			return true;
		}else{
			return false;
		}
	}
	
	public boolean isFilterLayer(String layer,String filter_name){
		if (null != filter_layers && filter_layers.containsKey(filter_name)
				&& filter_layers.get(filter_name).contains(layer.toLowerCase())){
			return true;
		}else{
			return false;
		}
	}



}
