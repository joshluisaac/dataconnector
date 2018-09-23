package com.profitera.dc.parser.exception;

import com.profitera.util.exception.GenericException;

public class BatchNotExistException extends GenericException {

  private static final String ERROR_CODE = "DL_BATCH_DO_NOT_EXIST";

  public BatchNotExistException() {
    super("No batch with the specified name is configured.");
  }

  public BatchNotExistException(String message) {
    super(message);
  }

  public BatchNotExistException(Throwable cause) {
    super(cause);
  }

  public BatchNotExistException(String message, Throwable cause) {
    super(message, cause);
  }

  public String getErrorCode() {
    return ERROR_CODE;
  }
}
