/*
 * Created on May 4, 2005
 */
package com.profitera.dc.handler;

import java.util.Map;

import com.profitera.util.Strings;

public class IntegerHandler extends AbstractFieldTextHandler {

  public static final String SPACE = "space";
  private String posSign = " ";

  public void configure(String args) {
    if (args != null) {
      if (args.trim().equalsIgnoreCase(SPACE))
        posSign = " ";
      else
        posSign = args.trim();
    }
  }

  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText == null || fieldText.trim().length() == 0)
      return null;
    return new Integer(fieldText);
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    if (value == null) {
      if (defaultValue != null && defaultValue.length() != 0)
        return defaultValue;
      else if (length > 0)
        return posSign + Strings.leftPad("", length - 1, '0');
      else
        return null;
    }

    if (length > 0) {
      int tmpVal = ((Integer) value).intValue();
      if (tmpVal < 0)
        tmpVal = tmpVal * -1;
      String retVal = Strings.leftPad("" + tmpVal, length - 1, '0');
      if (((Integer) value).intValue() < 0)
        retVal = "-" + retVal;
      else
        retVal = posSign + retVal;
      return retVal;
    } else
      return ((Integer) value).toString();
  }

  public boolean isReversalSupported() {
    return true;
  }

  public Class getValueType() {
    return Integer.class;
  }

  public String getBehaviourDocumentation() {
    return "Interprets the text as a whole number.";
  }

  public String getConfigurationDocumentation() {
    return "None.";
  }
}
