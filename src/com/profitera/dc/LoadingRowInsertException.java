/*
 * Created on Mar 7, 2005
 */
package com.profitera.dc;

/**
 * @author jamison
 */
public class LoadingRowInsertException extends TransactionFailedException {

  /**
   * 
   */
  public LoadingRowInsertException() {
    super();
    
  }
  /**
   * @param message
   */
  public LoadingRowInsertException(String message) {
    super(message);
    
  }
  /**
   * @param message
   * @param cause
   */
  public LoadingRowInsertException(String message, Throwable cause) {
    super(message, cause);
    
  }
  /**
   * @param cause
   */
  public LoadingRowInsertException(Throwable cause) {
    super(cause);
    
  }
}
