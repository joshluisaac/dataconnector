package com.profitera.dc.handler;

import java.util.Map;

public abstract class AbstractFieldTextHandler implements IFieldTextHandler {
  public abstract Object getValue(String text, Map<String, String> allFields, IFieldTextHandlerContext context);
  @Override
  public Object getValue(String fieldText, Map<String, String> allFields, String defaultValue, IFieldTextHandlerContext context) {
    Object result = getValue(fieldText, allFields, context);
    if (result != null) {
      return result;
    } else {
      return getValue(defaultValue, allFields, context);
    }
  }
}
