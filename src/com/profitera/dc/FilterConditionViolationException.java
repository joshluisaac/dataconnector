package com.profitera.dc;

public class FilterConditionViolationException extends InvalidLineException {
  private static final long serialVersionUID = 1L;
  protected String field;
  protected Object value;
  protected Object condition;
  protected long rowNo;

  public FilterConditionViolationException(long rowNo, String field, Object value, Object condition) {
    super("Did not meet filter condition " + field + " one of/equals " + condition, rowNo);
    this.rowNo = rowNo;
    this.field = field;
    this.value = value;
    this.condition = condition;
  }
}
