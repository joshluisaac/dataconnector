/*
 * Created on Mar 7, 2005
 */
package com.profitera.dc;

/**
 * @author jamison
 */
public class MissingLookupException extends InvalidLineException {
  protected String column;
  protected String table;
  protected Object value;

  /**
   * @param message
   * @param lineNo
   */
  public MissingLookupException(String tab, String col, Object val, long lineNo) {
    this(tab, col, val, null, lineNo);
    
  }
  /**
   * @param message
   * @param cause
   * @param lineNo
   */
  public MissingLookupException(String tab, String col, Object val, Throwable cause, long lineNo) {
    super(null, cause, lineNo);
    column = col;
    table = tab;
    value = val;
  }
  
  
  public String getMessage() {
    if (super.getMessage() == null)
      return "Lookup data with conditions [" + column + "]" + " for record at line " + getLineNumber() + " not found.";
    else
      return super.getMessage();
  }
  
}
