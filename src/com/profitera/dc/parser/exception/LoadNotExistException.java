package com.profitera.dc.parser.exception;

import com.profitera.util.exception.GenericException;

public class LoadNotExistException extends GenericException {

  private static final String ERROR_CODE = "DL_LOAD_NOT_EXIST";

  public LoadNotExistException() {
    super("No load with the specified name is configured.");
  }

  public LoadNotExistException(String message) {
    super(message);
  }

  public LoadNotExistException(Throwable cause) {
    super(cause);
  }

  public LoadNotExistException(String message, Throwable cause) {
    super(message, cause);
  }

  public String getErrorCode(){
    return ERROR_CODE;
  }
}