package com.profitera.dc.impl;

import com.profitera.dc.handler.IFieldTextHandlerContext;

public class DefaultFieldTextHandlerContext implements IFieldTextHandlerContext {
  private final String fieldName;
  private final long line;
  private String load;

  public DefaultFieldTextHandlerContext(String fieldName, long line, String load) {
    this.fieldName = fieldName;
    this.line = line;
    this.load = load;
  }
  @Override
  public String getCurrentFieldName() {
    return fieldName;
  }

  @Override
  public long getCurrentLineNumber() {
    return line;
  }

  @Override
  public String getLoadName() {
    return load;
  }

}
