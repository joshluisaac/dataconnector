/*
 * Created on Mar 7, 2005
 */
package com.profitera.dc;

/**
 * @author jamison
 */
public class QueryExistingRowException extends InvalidLineException {
  /**
   * @param message
   * @param lineNo
   */
  public QueryExistingRowException(String message, long lineNo) {
    super(message, lineNo);
  }
  /**
   * @param message
   * @param cause
   * @param lineNo
   */
  public QueryExistingRowException(String message, Throwable cause, long lineNo) {
    super(message, cause, lineNo);
  }
}
