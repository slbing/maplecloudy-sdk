package org.elasticsearch.hadoop.yarn.am;

import org.elasticsearch.hadoop.yarn.EsYarnException;

public class EsYarnAmException extends EsYarnException {

    /**
   * 
   */
  private static final long serialVersionUID = 1L;

    public EsYarnAmException() {
    }

    public EsYarnAmException(String message, Throwable cause) {
        super(message, cause);
    }

    public EsYarnAmException(String message) {
        super(message);
    }

    public EsYarnAmException(Throwable cause) {
        super(cause);
    }

}
