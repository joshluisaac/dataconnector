package com.profitera.dc.handler;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.profitera.util.Strings;

public class TimeHandler extends AbstractFieldTextHandler {
  private SimpleDateFormat formatter = new SimpleDateFormat("HHmmss");
  public void configure(String args) {
    if (args != null && args.trim().length() != 0)
      formatter = new SimpleDateFormat(args);
  }
  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText != null && fieldText.equalsIgnoreCase("CURRENT_TIME")) {
      return new Time(System.currentTimeMillis());
    }
    int fmtLength = formatter.toPattern().length();
    String nullTimeString = new String();
    for (int i = 0; i < fmtLength; i++) {
      nullTimeString += "0";
    }
    if (fieldText == null || fieldText.trim().length() == 0 || fieldText.indexOf(nullTimeString) != -1) {
      return null;
    }
    return parseDate(fieldText);
  }

  /**
   * @param fieldText
   * @return
   */
  private Date parseDate(String fieldText) {
    formatter.setLenient(false);
    try {
      return new Time(formatter.parse(fieldText).getTime());
    } catch (ParseException e) {
      log.warn("Parsing failed for time value " + fieldText + " with pattern " + formatter.toPattern());
      log.warn("Error is: " + e.getMessage());
      return null;
    }
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
    return formatter.format((Time) value);
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
    return Time.class;
  }

  public String getBehaviourDocumentation() {
    return "Used to insert a time field into the database";
  }

  public String getConfigurationDocumentation() {
    return "A time format, defaults to 'HHmmss'.";
  }
}
