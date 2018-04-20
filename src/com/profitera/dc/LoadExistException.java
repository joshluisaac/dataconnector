package com.profitera.dc;

import com.profitera.util.exception.GenericException;

public class LoadExistException extends GenericException {

  private static String ERROR_CODE = "DL_LOAD_EXIST";
  int flag=0;
  
  public LoadExistException() {
    super();
  }

  public LoadExistException(String message) {
    super(message);
  }

  public LoadExistException(String message, String errorCode) {
    super(message, errorCode);
    ERROR_CODE=errorCode;
    flag=1;
  }

  public LoadExistException(Throwable cause) {
    super(cause);
  }

  public LoadExistException(String message, Throwable cause) {
    super(message, cause);
  }

  public String getErrorCode(){
	  if (flag==0)
		 {
	    	return ERROR_CODE="DL_LOAD_EXIST";
		 }
		 else 
		 {
			 flag=0;
			 return ERROR_CODE;
		 }
		 
  }
}
