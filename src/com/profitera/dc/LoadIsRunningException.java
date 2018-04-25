package com.profitera.dc;

import com.profitera.util.exception.GenericException;

public class LoadIsRunningException extends GenericException {

  private static String ERROR_CODE = "DL_LOAD_RUNNING";
  int flag=0;
  
  public LoadIsRunningException() {
    super();
  }

  public LoadIsRunningException(String message) {
    super(message);
  }

  public LoadIsRunningException(String message, String errorCode) {
    super(message, errorCode);
    ERROR_CODE=errorCode;
    flag=1;
  }

  public LoadIsRunningException(Throwable cause) {
    super(cause);
  }

  public LoadIsRunningException(String message, Throwable cause) {
    super(message, cause);
  }

  public String getErrorCode(){
	  if (flag==0)
		 {
	    	return ERROR_CODE="DL_LOAD_RUNNING";
		 }
		 else 
		 {
			 flag=0;
			 return ERROR_CODE;
		 }
		 
  }
}
