/*
 * Created on Mar 7, 2005
 */
package com.profitera.dc;

/**
 * @author jamison
 */
public class LoadingRowUpdateException extends TransactionFailedException {

  /**
   * 
   */
  public LoadingRowUpdateException() {
    super();
    
  }
  /**
   * @param message
   */
  public LoadingRowUpdateException(String message) {
    super(message);
    
  }
  /**
   * @param message
   * @param cause
   */
  public LoadingRowUpdateException(String message, Throwable cause) {
    super(message, cause);
    
  }
  /**
   * @param cause
   */
  public LoadingRowUpdateException(Throwable cause) {
    super(cause);
    
  }
}
