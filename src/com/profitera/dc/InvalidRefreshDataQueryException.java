package com.profitera.dc;

import com.profitera.util.exception.GenericException;

public class InvalidRefreshDataQueryException extends GenericException {

  private final static String ERROR_CODE = "DL_INVALID_REFRESH_QRY";
  
  String query;
  
  private InvalidRefreshDataQueryException() {
    super();
  }

  public InvalidRefreshDataQueryException(String query) {
    super("Error code: " + ERROR_CODE + "\n" + "Query: " + query);
    this.query = query;
  }

  public String getErrorCode(){
    return ERROR_CODE;
  }
  
  public String getMessage(){
    String message = new String();
    message += "Exception: Invalid Refresh Data Query\n";
    message += "Error Code: " + getErrorCode() +"\n";
    message += "Query: " + query + "\n";
    return message;
  }
}