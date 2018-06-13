/*
 * Created on May 4, 2005
 */
package com.profitera.dc.handler;

import java.util.Map;

import com.profitera.util.Strings;

public class LeftTrimHandler extends AbstractFieldTextHandler {

  String trimChar = "0";

  public void configure(String args) {
    if (args != null && args.trim().length() != 0) {
      trimChar = args.trim();
    }
  }

  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText == null || fieldText.length() == 0) {
      return fieldText;
    }
    return Strings.leftTrim(fieldText, trimChar);
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    boolean isNull = value == null;
    boolean isZeroLength = isNull || ((String)value).length() == 0;
    if (isZeroLength) {
      if (defaultValue != null && defaultValue.length() != 0) {
        return defaultValue;
      } else if (length > 0) {
        return Strings.pad(trimChar, length);
      }
    }
    return Strings.leftPad((String) value, length, trimChar.charAt(0));
  }

  public boolean isReversalSupported() {
    return true;
  }

  public Class getValueType() {
    return String.class;
  }

  public String getBehaviourDocumentation() {
    return "Removes specified character at the left side of a string.";
  }

  public String getConfigurationDocumentation() {
    return "The character to remove.";
  }
}
