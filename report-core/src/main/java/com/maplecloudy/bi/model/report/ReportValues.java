package com.maplecloudy.bi.model.report;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.avro.generic.GenericData;

import com.google.common.collect.Maps;

public class ReportValues implements ReportV {
	private HashMap<String, Long> mergeVals = Maps.newHashMap();
	private HashMap<String, String> stringVals = Maps.newHashMap();
	private HashMap<String, Double> maxVals = Maps.newHashMap();
	private HashMap<String, Double> minVals = Maps.newHashMap();
	private HashMap<String, AvgVal> avgVals = Maps.newHashMap();
	private HashMap<String, Long[]> distVals = Maps.newHashMap();
	
	public static class AvgVal{
		private long cnt = 0;
		private double sum = 0.0;
		
		public void add(double v){
			cnt ++;
			sum += v;
		}
		
		public void merge(AvgVal o){
			cnt += o.cnt;
			sum += o.sum;
		}
		
		public Double getAvg(){
			if (cnt >0){
				return sum/cnt;
			}else{
				return Double.MIN_VALUE;
			}
		}
	}

	public void merge(ReportValues other) {
		if (this.mergeVals == null) {
			mergeVals = Maps.newHashMap();
			this.mergeVals.putAll(other.mergeVals);
			return;
		}
		for (Entry<String, Long> entry : other.mergeVals.entrySet()) {
			if (this.mergeVals.containsKey(entry.getKey())) {
				this.mergeVals.put(entry.getKey(), this.mergeVals.get(entry.getKey()) + entry.getValue());
			} else {
				this.mergeVals.put(entry.getKey(), entry.getValue());
			}
		}

		for (Entry<String, Double> entry : other.maxVals.entrySet()) {
			if (this.maxVals.containsKey(entry.getKey())) {
				this.maxVals.put(entry.getKey(), Math.max(this.maxVals.get(entry.getKey()) , entry.getValue()));
			} else {
				this.maxVals.put(entry.getKey(), entry.getValue());
			}
		}
		
		for (Entry<String, Double> entry : other.minVals.entrySet()) {
			if (this.minVals.containsKey(entry.getKey())) {
				this.minVals.put(entry.getKey(), Math.min(this.minVals.get(entry.getKey()) , entry.getValue()));
			} else {
				this.minVals.put(entry.getKey(), entry.getValue());
			}
		}
		
		for (Entry<String, AvgVal> entry : other.avgVals.entrySet()) {
			if (this.avgVals.containsKey(entry.getKey())) {
				this.avgVals.get(entry.getKey()).merge(entry.getValue());
			} else {
				this.avgVals.put(entry.getKey(), entry.getValue());
			}
		}
		
		for (Entry<String, Long[]> entry : other.distVals.entrySet()) {
			if (this.distVals.containsKey(entry.getKey())) {
				@SuppressWarnings("rawtypes")
				GenericData.Array distVals = (GenericData.Array)(Object)entry.getValue();
				Long[] distThat = new Long[distVals.size()];
				for (int i=0; i<distThat.length; ++i){
					distThat[i] = (Long) distVals.get(i);
				}

				Long[] distThis = this.distVals.get(entry.getKey());
//				Long[] distThat = entry.getValue();
				for(int i=0; i<distThis.length && i<distThat.length; ++i){
					distThis[i] += distThat[i];
				}
			} else {
				@SuppressWarnings("rawtypes")
				GenericData.Array distVals = (GenericData.Array)(Object)entry.getValue();
				Long[] vals = new Long[distVals.size()];
				for (int i=0; i<vals.length; ++i){
					vals[i] = (Long) distVals.get(i);
				}
				this.distVals.put(entry.getKey(), vals);
			}
	}
	}
	

	@Override
	public String toString() {
		return "ReportValues [mergeVals=" + mergeVals + "]";
	}

	public void putMerge(String key, Long value) {
		if (null == this.mergeVals) {
			mergeVals = Maps.newHashMap();
		}

		mergeVals.put(key, value);
	}

	public void putSum(String key, Long value){
		putMerge(key,value);
	}
	
	public void addSum(String key, Long value){
		if (this.mergeVals.containsKey(key)) {
			this.mergeVals.put(key, this.mergeVals.get(key) + value);
		} else {
			this.mergeVals.put(key, value);
		}
	}
	
	public void putMax(String key, Double value){
		if (null == this.maxVals) {
			maxVals = Maps.newHashMap();
		}

		maxVals.put(key, value);
	}
	
	public void addMax(String key, Double value){
		if (this.maxVals.containsKey(key) && null!=this.maxVals.get(key)) {
			this.maxVals.put(key, Math.max(this.maxVals.get(key) , value));
		} else {
			this.maxVals.put(key, value);
		}
	}
	
	public void putMin(String key, Double value){
		if (null == this.minVals) {
			minVals = Maps.newHashMap();
		}

		minVals.put(key, value);
	}
	
	public void addMin(String key, Double value){
		if (this.minVals.containsKey(key) && null!=this.minVals.get(key)) {
			this.minVals.put(key, Math.min(this.minVals.get(key) , value));
		} else {
			this.minVals.put(key, value);
		}
	}
	
	public void putAvg(String key, Double value){
		if (null == this.avgVals) {
			avgVals = Maps.newHashMap();
		}

		AvgVal avg = new AvgVal();
		avg.add(value);
		avgVals.put(key, avg);
	}
	
	public void addAvg(String key, Double value){
		if (!this.avgVals.containsKey(key)) {
			AvgVal avg = new AvgVal();
			this.avgVals.put(key, avg);
		}
		
		this.avgVals.get(key).add(value);
	}
	
	public void putDist(String key, int index, Long value, int size){
		if (null == this.distVals) {
			distVals = Maps.newHashMap();
		}
		
		if (size>0 && size>index && index >=0){
			Long[] vals = new Long[size];
			for (int i=0; i<size; ++i){
				vals[i] = 0L;
			}
			vals[index] = value;
			
			distVals.put(key, vals);
		}
	}
	
	public void addDist(String key, int index, Long value, int size){
		if (size>0 && size>index && index >=0){
			if (null == distVals.get(key)){
				putDist(key,index,value,size);
			}else{
				Long[] vals = distVals.get(key);
				if (vals.length > index){
					if (null == vals[index]){
						vals[index] = value;
					}else{
						vals[index] += value;
					}
				}
			}
		}
	}
	
	public Long getMerge(String key) {
		if (null == mergeVals) {
			return 0L;
		}

		return mergeVals.get(key) == null ? 0L : mergeVals.get(key);
	}
	
	public Double getMin(String key){
		return minVals.get(key)==null ? 0.0 : minVals.get(key);
	}
	
	public Double getMax(String key){
		return maxVals.get(key)==null ? 0.0 : maxVals.get(key);
	}
	
	public Double getAvg(String key){
		return avgVals.get(key)==null ? 0.0 : avgVals.get(key).getAvg();
	}

	public String getDist(String key){
		if (null == distVals.get(key)){
			return null;
		}
		@SuppressWarnings("rawtypes")
		GenericData.Array dVals = (GenericData.Array)(Object)(distVals.get(key));
		if (null == dVals) return null;
		Long[] vals = new Long[dVals.size()];
		for (int i=0; i<vals.length; ++i){
			vals[i] = (Long) dVals.get(i);
		}

//		Long[] vals = distVals.get(key);
//		if (null == vals) return null;
		
		StringBuilder sb = new StringBuilder();
		if (vals.length>0){
			sb.append(vals[0]);
			for (int i=1; i<vals.length; ++i){
				sb.append(",");
				sb.append(vals[i]);
			}
		}
		return sb.toString();
	}

	public void put(String key, String value) {
		if (null == this.stringVals) {
			stringVals = Maps.newHashMap();
		}

		stringVals.put(key, value);
	}

	/* 需要支持当合并使用时, 无key时value取0  */
	public String get(String key) {
		if (mergeVals.containsKey(key)) return mergeVals.get(key).toString();
		else if (minVals.containsKey(key)) return getMin(key).toString();
		else if (maxVals.containsKey(key)) return getMax(key).toString();
		else if (avgVals.containsKey(key)) return getAvg(key).toString();
		// 
		else if ((null==stringVals || stringVals.size()<=0) && 
				(null==distVals || distVals.size()<=0)){
			return ""+0;
		}
		else if (stringVals.containsKey(key)) return stringVals.get(key);
		else if (distVals.containsKey(key)) return getDist(key);

		return "";
	}

	public HashMap<String, Long> getMap() {
		return mergeVals;
	}

	public void clear() {
		if (null != this.mergeVals) {
			this.mergeVals.clear();
		}

		if (null != this.stringVals) {
			this.stringVals.clear();
		}
	}

	public int size() {
		int size = 0;
		
		if (null != this.mergeVals) {
			size += this.mergeVals.size();
		}

		if (null != this.stringVals) {
			size += this.stringVals.size();
		}
		
		size += this.minVals.size();
		size += this.maxVals.size();
		size += this.avgVals.size();
		size += this.distVals.size();
		
		return size;
	}
}
