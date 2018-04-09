package com.profitera.dc;

import com.profitera.util.exception.GenericException;

public class BatchIsNotRunningException extends GenericException {

  private static String ERROR_CODE = "DL_BATCH_NOT_RUNNING";
  int flag=0;
  
  public BatchIsNotRunningException() {
    super();
  }

  public BatchIsNotRunningException(String message) {
    super(message);
  }

  public BatchIsNotRunningException(String message, String errorCode) {
    super(message, errorCode);
    ERROR_CODE=errorCode;
    flag=1;
  }

  public BatchIsNotRunningException(Throwable cause) {
    super(cause);
  }

  public BatchIsNotRunningException(String message, Throwable cause) {
    super(message, cause);
  }

  public String getErrorCode(){
	  if (flag==0)
		 {
	    	return ERROR_CODE="DL_BATCH_NOT_RUNNING";
		 }
		 else 
		 {
			 flag=0;
			 return ERROR_CODE;
		 }
		 
  }
}
