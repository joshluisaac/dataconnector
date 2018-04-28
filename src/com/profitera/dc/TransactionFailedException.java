/*
 * Created on Mar 7, 2005
 */
package com.profitera.dc;

/**
 * @author jamison
 */
public class TransactionFailedException extends RuntimeException {

  /**
   * 
   */
  public TransactionFailedException() {
    super();
    
  }
  /**
   * @param message
   */
  public TransactionFailedException(String message) {
    super(message);
    
  }
  /**
   * @param message
   * @param cause
   */
  public TransactionFailedException(String message, Throwable cause) {
    super(message, cause);
    
  }
  /**
   * @param cause
   */
  public TransactionFailedException(Throwable cause) {
    super(cause);
    
  }
}
