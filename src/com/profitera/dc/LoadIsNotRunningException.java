package com.profitera.dc;

import com.profitera.util.exception.GenericException;

public class LoadIsNotRunningException extends GenericException {
  private static String ERROR_CODE = "DL_LOAD_NOT_RUNNING";
  int flag=0;
  
  public LoadIsNotRunningException() {
    super();
  }

  public LoadIsNotRunningException(String message) {
    super(message);
  }

  public LoadIsNotRunningException(String message, String errorCode) {
    super(message, errorCode);
    ERROR_CODE=errorCode;
    flag=1;
  }

  public LoadIsNotRunningException(Throwable cause) {
    super(cause);
  }

  public LoadIsNotRunningException(String message, Throwable cause) {
    super(message, cause);
  }
  
  public String getErrorCode(){
	  if (flag==0)
		 {
	    	return ERROR_CODE="DL_LOAD_NOT_RUNNING";
		 }
		 else 
		 {
			 flag=0;
			 return ERROR_CODE;
		 }
		 
  }

}
