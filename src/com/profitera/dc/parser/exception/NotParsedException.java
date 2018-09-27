package com.profitera.dc.parser.exception;

import com.profitera.util.exception.GenericException;

public class NotParsedException extends GenericException {

  private static final String ERROR_CODE = "DL_CONFIG_NOT_PARSED";
    
  public NotParsedException() {
    super("No configuration parsed yet, please provide configuration file and parse using parse() or parse(Properties) methods.");
  }

  public NotParsedException(String message) {
    super(message);
  }

  public NotParsedException(Throwable cause) {
    super(cause);
  }

  public NotParsedException(String message, Throwable cause) {
    super(message, cause);
  }
  
  public String getErrorCode() {
    return ERROR_CODE;
  }
}