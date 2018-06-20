package com.profitera.dc.handler;

import java.util.Map;

import com.profitera.util.Strings;

public class StringHandler extends AbstractFieldTextHandler {

  public void configure(String args) {

  }

  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText == null || fieldText.trim().length() == 0) {
      return null;
    }
    return fieldText;
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    if (value == null) {
      if (defaultValue != null && defaultValue.length() != 0)
        return defaultValue;
      else if (length > 0)
        return Strings.pad("", length);
      else
        return null;
    }
    return (length > 0 ? Strings.pad((String) value, length) : (String) value);
  }

  public boolean isReversalSupported() {
    return true;
  }
  public Class getValueType() {
    return String.class;
  }

  public String getBehaviourDocumentation() {
    return " Do not have any special qualities, exist to allow customizer to specify the this handler when a field has lookup query and the lookup query return type (lookupType) is String.";
  }

  public String getConfigurationDocumentation() {
    return "None";
  }
}
