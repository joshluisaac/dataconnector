/*
 * Created on Mar 9, 2005
 */
package com.profitera.dc.handler;

import java.util.Map;

import com.profitera.util.Strings;

public class LongHandler extends AbstractFieldTextHandler {
  public static final String SPACE = "space";
  private String posSign = " ";

  public void configure(String args) {
    if (args != null) {
      if (args.trim().equalsIgnoreCase(SPACE)) {
        posSign = " ";
      } else {
        posSign = args.trim();
      }
    }
  }

  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (Strings.nullifyIfBlank(fieldText) == null) {
      return null;
    }
    return Long.valueOf(fieldText);
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    if (value == null) {
      if (Strings.nullifyIfBlank(defaultValue) == null) {
        if (length > 0) {
          return posSign + Strings.leftPad("", length - 1, '0');
        } else {
          return null;
        }
      } else {
        return defaultValue;
      }
    }

    if (length > 0) {
      boolean isPositive = ((Long) value).longValue() >= 0;
      String retVal;
      if (isPositive) {
        retVal = ((Long) value).toString();
      } else {
        retVal = Long.toString(((Long) value).longValue() * -1);
      }
      retVal = Strings.leftPad(retVal, length - 1, '0');
      if (isPositive) {
        retVal = posSign + retVal;
      } else {
        retVal = "-" + retVal;
      }
      return retVal;
    } else {
      return ((Long) value).toString();
    }
  }

  public boolean isReversalSupported() {
    return true;
  }

  public Class<?> getValueType() {
    return Long.class;
  }

  public String getBehaviourDocumentation() {
    return "Interprets the text as a whole number.";
  }

  public String getConfigurationDocumentation() {
    return "None.";
  }
}
