package com.profitera.dc.handler;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import com.profitera.util.DateParser;
import com.profitera.util.Strings;

public class DateRollHandler extends AbstractFieldTextHandler {

  public static String ROLL_FORWARD = "FORWARD";
  public static String ROLL_BACKWARD = "BACKWARD";

  int rollDirection;

  public void configure(String args) {
    if (args != null && args.trim().length() != 0)
      if (args.equalsIgnoreCase("BACKWARD"))
        rollDirection = -1;
      else
        rollDirection = 1;
  }

  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText == null || fieldText.length() == 0) {
      return null;
    }
    int daysToRoll = Integer.parseInt(fieldText) * rollDirection;
    Calendar today = Calendar.getInstance();
    today.add(Calendar.DATE, daysToRoll);
    return today.getTime();
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    if (value == null) {
      if (defaultValue != null && defaultValue.length() != 0) {
        return defaultValue;
      } else if (length > 0) {
        return Strings.leftPad("", length, '0');
      } else {
        return null;
      }
    }
    Calendar valueCal = Calendar.getInstance();
    valueCal.setTime((Date) value);
    long days = DateParser.getDaysDifference(Calendar.getInstance(), valueCal);
    days = days * rollDirection;
    long unsignedDays = days < 0 ? days * -1 : days;

    if (length > 0) {
      String daysStr = Strings.leftPad(String.valueOf(unsignedDays), length - 1, '0');
      daysStr = (days < 0 ? "-" : " ") + daysStr;
      return daysStr;
    } else {
      return Long.toString(days);
    }
  }

  public boolean isReversalSupported() {
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.profitera.dc.handler.IFieldTextHandler#getValueType()
   */
  public Class getValueType() {
    return Date.class;
  }

  public String getBehaviourDocumentation() {
    return " To provide ability of rolling dates based on the number of days specified in the field of a file. When a field is of number type, and this handler is specified for it, the field value will be used to roll current date backward or forward for the number of days the field represent. The direction of rolling, whether backward or forward is specified as argument.";
  }

  public String getConfigurationDocumentation() {
    return "FORWARD - Indicating moving direction to be forward (today to future), BACKWARD - Indicating moving direction to be backward (today to past)";
  }
}
