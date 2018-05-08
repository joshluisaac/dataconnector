package com.profitera.dc;

public class InvalidLookupDefinitionException extends Exception {

  public InvalidLookupDefinitionException(String message) {
    super(message);
  }
  /**
   * @param message
   * @param cause
   */
  public InvalidLookupDefinitionException(String message, Throwable cause) {
    super(message, cause);
  }
}