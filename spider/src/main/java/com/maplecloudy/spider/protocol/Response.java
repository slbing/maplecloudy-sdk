package com.maplecloudy.spider.protocol;

// JDK imports
import java.net.URL;

import com.maplecloudy.spider.metadata.HttpHeaders;


// Nutch imports


/**
 * A response inteface.  Makes all protocols model HTTP.
 */
public interface Response extends HttpHeaders {
  
  /** Returns the URL used to retrieve this response. */
  public URL getUrl();

  /** Returns the response code. */
  public int getCode();

  /** Returns the value of a named header. */
  public String getHeader(String name);

  /** Returns all the headers. */
  public java.util.Map<String,java.util.List<String>> getHeaders();
  
  /** Returns the full content of the response. */
  public byte[] getContent();

}
