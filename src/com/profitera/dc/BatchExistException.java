package com.profitera.dc;

import com.profitera.util.exception.GenericException;

public class BatchExistException extends GenericException {

  private static String ERROR_CODE = "DL_BATCH_EXIST";
  int flag=0;
  
  public BatchExistException() {
    super();
  }

  public BatchExistException(String message) {
    super(message);
  }

  public BatchExistException(String message, String errorCode) {
    super(message, errorCode);
    ERROR_CODE=errorCode;
    flag=1;
  }

  public BatchExistException(Throwable cause) {
    super(cause);
  }

  public BatchExistException(String message, Throwable cause) {
    super(message, cause);
  }

  public String getErrorCode(){
	 if (flag==0)
	 {
    	return ERROR_CODE="DL_BATCH_EXIST";
	 }
	 else 
	 {
		 flag=0;
		 return ERROR_CODE;
	 }
  }
}
