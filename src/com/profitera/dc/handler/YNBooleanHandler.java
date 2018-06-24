/*
 * Created on Mar 9, 2005
 */
package com.profitera.dc.handler;

import java.util.Map;

import com.profitera.util.Strings;

public class YNBooleanHandler extends AbstractFieldTextHandler {
  private String trueIndicator = "Y";

  public void configure(String args) {
    if (args != null) {
      trueIndicator = args;
    }
  }

  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText != null && fieldText.equalsIgnoreCase(trueIndicator)) {
      return Boolean.TRUE;
    }
    return Boolean.FALSE;
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    if (value == null) {
      if (defaultValue != null && defaultValue.length() != 0) {
        return defaultValue;
      } else if (length > 0) {
        return Strings.pad("", length);
      } else {
        return null;
      }
    }
    if (length > 0) {
      return ((Integer) value).intValue() == 1 ? Strings.pad(trueIndicator, length) : Strings.pad("N", length);
    } else {
      return ((Integer) value).intValue() == 1 ? trueIndicator : "N";
    }
  }

  public boolean isReversalSupported() {
    return true;
  }

  public Class getValueType() {
    return Boolean.class;
  }

  public String getBehaviourDocumentation() {
    return "Interprets the text as a Yes/No value, 'Y' is yes (true) and 'N' is no (false).";
  }

  public String getConfigurationDocumentation() {
    return "None";
  }
}
