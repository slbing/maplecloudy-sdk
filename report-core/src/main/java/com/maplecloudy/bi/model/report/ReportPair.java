package com.maplecloudy.bi.model.report;

import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.maplecloudy.avro.util.AvroUtils;
import com.maplecloudy.bi.util.ReportUtils;

public class ReportPair<RK extends ReportKey> {
	public RK key;
	public ReportValues reportValues;

	// public ReportPair() {}
	Configuration conf;

	public ReportPair(RK key, Configuration conf) {
		this.key = key;
		this.reportValues = new ReportValues();
		this.conf = conf;
	}

	public ReportPair(RK key, ReportValues value, Configuration conf) {
		this.key = key;
		this.reportValues = value;
		this.conf = conf;
	}

	public void merge(ReportPair<RK> other) throws Exception {
		if (!this.key.equals(other.key))
			throw new Exception("can't merge different report!");
		if (other == null || other.reportValues == null)
			return;
		if (this.reportValues == null)
			this.reportValues = new ReportValues();
		this.reportValues.merge(other.reportValues);
	}

	public String toString() {
		return AvroUtils.toAvroString(this);
	}

	public void put(String key, Long value) {
		TreeSet<String> ts = ReportUtils.getAllValueNames(this.key.getClass(),
				conf);
		if (!ts.contains(key))
			throw new RuntimeException(key + " not in @"
					+ this.key.getClass().getName() + " declare names:"
					+ StringUtils.join(ts, ","));
		if (this.reportValues == null)
			reportValues = new ReportValues();
		reportValues.putMerge(key, value);
	}

	public void putSum(String key, Long value) {
		TreeSet<String> ts = ReportUtils.getAllValueNames(this.key.getClass(),
				conf);
		if (!ts.contains(key))
			throw new RuntimeException(key + " not in @"
					+ this.key.getClass().getName() + " declare names:"
					+ StringUtils.join(ts, ","));
		if (this.reportValues == null)
			reportValues = new ReportValues();
		reportValues.putSum(key, value);
	}
	
	public void addSum(String key, Long value) {
		TreeSet<String> ts = ReportUtils.getAllValueNames(this.key.getClass(),
				conf);
		if (!ts.contains(key))
			throw new RuntimeException(key + " not in @"
					+ this.key.getClass().getName() + " declare names:"
					+ StringUtils.join(ts, ","));
		if (this.reportValues == null)
			reportValues = new ReportValues();
		reportValues.addSum(key, value);
	}

	public void putMin(String key, Double value) {
		TreeSet<String> ts = ReportUtils.getAllValueNames(this.key.getClass(),
				conf);
		if (!ts.contains(key))
			throw new RuntimeException(key + " not in @"
					+ this.key.getClass().getName() + " declare names:"
					+ StringUtils.join(ts, ","));
		if (this.reportValues == null)
			reportValues = new ReportValues();
		reportValues.putMin(key, value);
	}
	
	public void addMin(String key, Double value) {
		TreeSet<String> ts = ReportUtils.getAllValueNames(this.key.getClass(),
				conf);
		if (!ts.contains(key))
			throw new RuntimeException(key + " not in @"
					+ this.key.getClass().getName() + " declare names:"
					+ StringUtils.join(ts, ","));
		if (this.reportValues == null)
			reportValues = new ReportValues();
		reportValues.addMin(key, value);
	}

	public void putMax(String key, Double value) {
		TreeSet<String> ts = ReportUtils.getAllValueNames(this.key.getClass(),
				conf);
		if (!ts.contains(key))
			throw new RuntimeException(key + " not in @"
					+ this.key.getClass().getName() + " declare names:"
					+ StringUtils.join(ts, ","));
		if (this.reportValues == null)
			reportValues = new ReportValues();
		reportValues.putMax(key, value);
	}

	public void addMax(String key, Double value) {
		TreeSet<String> ts = ReportUtils.getAllValueNames(this.key.getClass(),
				conf);
		if (!ts.contains(key))
			throw new RuntimeException(key + " not in @"
					+ this.key.getClass().getName() + " declare names:"
					+ StringUtils.join(ts, ","));
		if (this.reportValues == null)
			reportValues = new ReportValues();
		reportValues.addMax(key, value);
	}

	public void putAvg(String key, Double value) {
		TreeSet<String> ts = ReportUtils.getAllValueNames(this.key.getClass(),
				conf);
		if (!ts.contains(key))
			throw new RuntimeException(key + " not in @"
					+ this.key.getClass().getName() + " declare names:"
					+ StringUtils.join(ts, ","));
		if (this.reportValues == null)
			reportValues = new ReportValues();
		reportValues.putAvg(key, value);
	}

	public void addAvg(String key, Double value) {
		TreeSet<String> ts = ReportUtils.getAllValueNames(this.key.getClass(),
				conf);
		if (!ts.contains(key))
			throw new RuntimeException(key + " not in @"
					+ this.key.getClass().getName() + " declare names:"
					+ StringUtils.join(ts, ","));
		if (this.reportValues == null)
			reportValues = new ReportValues();
		reportValues.addAvg(key, value);
	}

	public void putDist(String key, int index, int size){
		TreeSet<String> ts = ReportUtils.getAllValueNames(this.key.getClass(),
				conf);
		if (!ts.contains(key))
			throw new RuntimeException(key + " not in @"
					+ this.key.getClass().getName() + " declare names:"
					+ StringUtils.join(ts, ","));
		if (this.reportValues == null)
			reportValues = new ReportValues();
		reportValues.putDist(key, index, 1L, size);
	}

	public void addDist(String key, int index, int size){
		TreeSet<String> ts = ReportUtils.getAllValueNames(this.key.getClass(),
				conf);
		if (!ts.contains(key))
			throw new RuntimeException(key + " not in @"
					+ this.key.getClass().getName() + " declare names:"
					+ StringUtils.join(ts, ","));
		if (this.reportValues == null)
			reportValues = new ReportValues();
		reportValues.addDist(key, index, 1L, size);
	}

	public void putWithoutCheck(String key, Long value) {
		if (this.reportValues == null)
			reportValues = new ReportValues();
		reportValues.putMerge(key, value);
	}

//	public Long get(String key) {
//		return reportValues.getMerge(key);
//	}
	
	
}
