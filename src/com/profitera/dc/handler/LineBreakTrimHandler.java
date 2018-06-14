package com.profitera.dc.handler;

import java.util.Map;

import com.profitera.util.Strings;

public class LineBreakTrimHandler extends AbstractFieldTextHandler {

  public void configure(String args) {

  }

  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    return getValue(fieldText, allFields);
  }
  private Object getValue(String fieldText, Map allFields) {
    if (fieldText == null || fieldText.trim().length() == 0) {
      return null;
    }
    fieldText = fieldText.replaceAll("\n", " ");
    fieldText = fieldText.replaceAll("\r", "");
    return fieldText.trim();
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    String ret = (String) getValue((String) value, allFields);
    if (ret == null) {
      if (length > 0) {
        return Strings.pad("", length);
      } else {
        return null;
      }
    }
    return (length > 0 ? Strings.pad((String) ret, length) : (String) ret);
  }

  public boolean isReversalSupported() {
    return true;
  }

  public Class getValueType() {
    return String.class;
  }

  public String getBehaviourDocumentation() {
    return "Line break trimmer, useful when a text field from hand off file has line breaks in between.";
  }

  public String getConfigurationDocumentation() {
    return "None.";
  }
}
