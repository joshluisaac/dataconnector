/*
 * Created on Mar 7, 2005
 */
package com.profitera.dc;

/**
 * @author jamison
 */
public class InvalidLineException extends RuntimeException {
  private long lineNo; 

  public InvalidLineException(String message, long lineNo) {
    super(message);
    this.lineNo = lineNo;
    
  }
  /**
   * @param message
   * @param cause
   */
  public InvalidLineException(String message, Throwable cause, long lineNo) {
    super(message, cause);
    this.lineNo = lineNo;
  }
  
  public long getLineNumber() {
    return lineNo;
  }
}
