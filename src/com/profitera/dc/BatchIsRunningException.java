package com.profitera.dc;

import com.profitera.util.exception.GenericException;

public class BatchIsRunningException extends GenericException {

  private static String ERROR_CODE = "DL_BATCH_RUNNING";
  int flag=0;
  
  public BatchIsRunningException() {
    super();
  }

  public BatchIsRunningException(String message) {
    super(message);
  }

  public BatchIsRunningException(String message, String errorCode) {
    super(message, errorCode);
    ERROR_CODE=errorCode;
    flag=1;
  }

  public BatchIsRunningException(Throwable cause) {
    super(cause);
  }

  public BatchIsRunningException(String message, Throwable cause) {
    super(message, cause);
  }
  
  public String getErrorCode(){
	  if (flag==0)
		 {
	    	return ERROR_CODE="DL_BATCH_RUNNING";
		 }
		 else 
		 {
			 flag=0;
			 return ERROR_CODE;
		 }
		 
  }
}