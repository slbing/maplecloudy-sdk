package com.maplecloudy.spider.crawl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.reflect.Nullable;

import com.maplecloudy.spider.schema.MD5;


/* The crawl state of a url. */
public class CrawlDatum implements Cloneable {
	public static final String								GENERATE_DIR_NAME					= "crawl_generate";
	public static final String								FETCH_DIR_NAME						= "crawl_fetch";
	public static final String								PARSE_DIR_NAME						= "crawl_parse";
	public final static String								PARSE_CLASS								= "parse_class";
	/** Page was not fetched yet. */
	public static final byte									STATUS_DB_UNFETCHED				= 0x01;
	/** Page was successfully fetched. */
	public static final byte									STATUS_DB_FETCHED					= 0x02;
	/** Page no longer exists. */
	public static final byte									STATUS_DB_GONE						= 0x03;
	/** Page temporarily redirects to other page. */
	public static final byte									STATUS_DB_REDIR_TEMP			= 0x04;
	/** Page permanently redirects to other page. */
	public static final byte									STATUS_DB_REDIR_PERM			= 0x05;
	/** Page was successfully fetched and found not modified. */
	public static final byte									STATUS_DB_NOTMODIFIED			= 0x06;

	/** Maximum value of DB-related status. */
	public static final byte									STATUS_DB_MAX							= 0x1f;

	/** Fetching was successful. */
	public static final byte									STATUS_FETCH_SUCCESS			= 0x21;
	/** Fetching unsuccessful, needs to be retried (transient errors). */
	public static final byte									STATUS_FETCH_RETRY				= 0x22;
	/** Fetching temporarily redirected to other page. */
	public static final byte									STATUS_FETCH_REDIR_TEMP		= 0x23;
	/** Fetching permanently redirected to other page. */
	public static final byte									STATUS_FETCH_REDIR_PERM		= 0x24;
	/** Fetching unsuccessful - page is gone. */
	public static final byte									STATUS_FETCH_GONE					= 0x25;
	/** Fetching successful - page is not modified. */
	public static final byte									STATUS_FETCH_NOTMODIFIED	= 0x26;

	/** Maximum value of fetch-related status. */
	public static final byte									STATUS_FETCH_MAX					= 0x3f;

	/** Page signature. */
	public static final byte									STATUS_SIGNATURE					= 0x41;
	/** Page was newly injected. */
	public static final byte									STATUS_INJECTED						= 0x42;
	/** Page discovered through a link. */
	public static final byte									STATUS_LINKED							= 0x43;
	/** Page got metadata from a parser */
	public static final byte									STATUS_PARSE_META					= 0x44;

	public static final HashMap<Byte, String>	statNames									= new HashMap<Byte, String>();
	static {
		statNames.put(STATUS_DB_UNFETCHED, "db_unfetched");
		statNames.put(STATUS_DB_FETCHED, "db_fetched");
		statNames.put(STATUS_DB_GONE, "db_gone");
		statNames.put(STATUS_DB_REDIR_TEMP, "db_redir_temp");
		statNames.put(STATUS_DB_REDIR_PERM, "db_redir_perm");
		statNames.put(STATUS_DB_NOTMODIFIED, "db_notmodified");
		statNames.put(STATUS_SIGNATURE, "signature");
		statNames.put(STATUS_INJECTED, "injected");
		statNames.put(STATUS_LINKED, "linked");
		statNames.put(STATUS_FETCH_SUCCESS, "fetch_success");
		statNames.put(STATUS_FETCH_RETRY, "fetch_retry");
		statNames.put(STATUS_FETCH_REDIR_TEMP, "fetch_redir_temp");
		statNames.put(STATUS_FETCH_REDIR_PERM, "fetch_redir_perm");
		statNames.put(STATUS_FETCH_GONE, "fetch_gone");
		statNames.put(STATUS_FETCH_NOTMODIFIED, "fetch_notmodified");
		statNames.put(STATUS_PARSE_META, "parse_metadata");

	}
	
	public int status;
  public long fetchTime;
  public long modifiedTime;
  public int fetchInterval;
  public int retries;
  public float score;
  @Nullable
  public MD5 signature;
  @Nullable
  public Map<String,String> extend;
  @Nullable
  public Map<String,String> metaData;
  
	public static boolean hasDbStatus(CrawlDatum datum) {
		if (datum.getStatus() <= STATUS_DB_MAX)
			return true;
		return false;
	}

	public static boolean hasFetchStatus(CrawlDatum datum) {
		if (datum.getStatus() > STATUS_DB_MAX
				&& datum.getStatus() <= STATUS_FETCH_MAX)
			return true;
		return false;
	}

	public CrawlDatum() {
	}
	public CrawlDatum( CrawlDatum datum) {
	  this.set(datum);
  }

	public CrawlDatum(int status, int fetchInterval) {
		this.status = status;
		this.fetchInterval = fetchInterval;
	}

	
	public CrawlDatum(int status, int fetchInterval, float score) {
		this(status, fetchInterval);
		this.score = score;
	}

	public int getStatus() {

		return this.status;
	}

	public static String getStatusName(byte value) {
		String res = statNames.get(value);
		if (res == null)
			res = "unknown";
		return res;
	}

	public void setStatus(int status) {

		this.status = status;
	}

	/**
	 * Returns either the time of the last fetch, or the next fetch time,
	 * depending on whether Fetcher or CrawlDbReducer set the time.
	 */
	public long getFetchTime() {
		return this.fetchTime;
	}

	/**
	 * Sets either the time of the last fetch or the next fetch time, depending on
	 * whether Fetcher or CrawlDbReducer set the time.
	 */
	public void setFetchTime(long fetchTime) {
		this.fetchTime = fetchTime;
	}

	public long getModifiedTime() {
		return this.modifiedTime;
	}

	public void setModifiedTime(long modifiedTime) {
		this.modifiedTime = modifiedTime;
	}

	public int getRetriesSinceFetch() {
		return this.retries;
	}

	public void setRetriesSinceFetch(int retries) {
		this.retries = retries;
	}

	public int getFetchInterval() {
		return this.fetchInterval;
	}

	public void setFetchInterval(int fetchInterval) {
		this.fetchInterval = fetchInterval;
	}

	public void setFetchInterval(float fetchInterval) {
		this.fetchInterval = Math.round(fetchInterval);
	}

	public float getScore() {
		return this.score;
	}

	public void setScore(float score) {
		this.score = score;
	}

	public byte[] getSignature() {
		if (this.signature != null)
			return this.signature.bytes();
		else
			return null;
	}

	public void setSignature(byte[] signature) {
		if(signature == null)
			return;
		if (signature != null && signature.length > 256)
			throw new RuntimeException("Max signature length (256) exceeded: "
					+ signature.length);
		if (this.signature == null)
			this.signature = new com.maplecloudy.spider.schema.MD5();
		this.signature.bytes(signature);

	}

	public void setMetaData(
			java.util.Map<String, String> meta) {
		this.metaData = meta;
	}

	public java.util.Map<String, String> getExtendData() {
		if (this.extend == null)
			this.extend = new java.util.HashMap<String, String>();
		return this.extend;
	}

	public void setExtendData(
			java.util.Map<String, String> extend) {
		this.extend = extend;
	}

	public void setExtend(String key, String value) {
		getExtendData().put(key, value);
	}

	public String getExtend(String key) {
		if (getExtendData().get(key) != null)
			return getExtendData().get(key).toString();
		else
			return null;
	}

	public void setMeta(String key, String value) {
		getMetaData().put(key, value);
	}

	/**
	 * Add all metadata from other CrawlDatum to datum Crawlthis.
	 * 
	 * @param other
	 *          CrawlDatum
	 */
	public void putAllMetaData(CrawlDatum other) {
		for (Entry<String, String> e : other
				.getMetaData().entrySet()) {
			getMetaData().put(e.getKey(), e.getValue());
		}
	}

	public void putAllExtendData(CrawlDatum other) {
		for (Entry<String, String> e : other
				.getExtendData().entrySet()) {
			getExtendData().put(e.getKey(), e.getValue());
		}
	}

	public java.util.Map<String, String> getMetaData() {
		if (this.metaData == null)
			this.metaData = new java.util.HashMap<String, String>();
		return this.metaData;
	}

	/** Copy the contents of another instance into datum instance. */
	public void set(CrawlDatum that) {
		this.status =that.status;
		this.fetchTime =that.fetchTime;
		this.retries =that.retries;
		this.fetchInterval =that.fetchInterval;
		this.score =that.score;
		this.modifiedTime =that.modifiedTime;
		this.signature =that.signature;
		if (that.metaData != null) {
			putAllMetaData(that); // make
		} else {
			this.metaData = null;
		}
		if (that.extend != null) {
			this.putAllExtendData(that);
		} else
			this.extend = null;
	}

	public Object clone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
}
