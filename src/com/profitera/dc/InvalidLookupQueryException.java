package com.profitera.dc;

import com.profitera.util.exception.GenericException;

public class InvalidLookupQueryException extends GenericException {

  private final static String ERROR_CODE = "DL_INVALID_LOOKUP_QRY";
  
  String query;
  String error;
  
  private InvalidLookupQueryException() {
    super();
  }

  public InvalidLookupQueryException(String query) {
    super("Error code: " + ERROR_CODE + "\n" + "Query: " + query);
    this.query = query;
  }

  public InvalidLookupQueryException(String query, String error) {
    super("Error code: " + ERROR_CODE + "\n" + "Query: " + query + (error != null ? "\n" + "Error: " + error : ""));
    this.query = query;
    this.error = error;
  }

  public String getErrorCode(){
    return ERROR_CODE;
  }
  
  public String getMessage(){
    String message = new String();
    message += "Exception: Invalid Lookup Query\n";
    message += "Error Code: " + getErrorCode() +"\n";
    message += "Query: " + query + "\n";
    if (error != null)
      message += "Message: " + error;
    return message;
  }
}