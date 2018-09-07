package com.profitera.dc.impl;

import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.parser.impl.FieldDefinition;

public interface IFieldTypeInfo {
  public String getDatabaseColumnDefinition(FieldDefinition field, int width);
  public int getFieldTextWidth(FieldDefinition field, IFieldTextHandler handler);
  public String render(Object value, int width);
}