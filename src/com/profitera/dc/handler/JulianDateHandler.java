/*
 * Created on Mar 9, 2005
 */
package com.profitera.dc.handler;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.profitera.util.Strings;

public class JulianDateHandler extends AbstractFieldTextHandler {
  private Map<String, Date> cache = new HashMap<>();

  public void configure(String args) {
  }

  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText == null || fieldText.trim().length() == 0 || "0000000".equals(fieldText))
      return null;
    Date d = (Date) cache.get(fieldText);
    if (d != null)
      return d;
    d = parseDate(fieldText);
    cache.put(fieldText, d);
    return d;
  }

  private Date parseDate(String fieldText) {
    DateFormat df = new SimpleDateFormat("yyyyDDD");
    df.setLenient(false);
    try {
      return df.parse(fieldText);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    if (value == null) {
      if (defaultValue != null && defaultValue.length() != 0)
        return defaultValue;
      else if (length > 0)
        return Strings.leftPad("", length, '0');
      else
        return null;
    }
    DateFormat df = new SimpleDateFormat("yyyyDDD");
    return df.format((Date) value);
  }

  public boolean isReversalSupported() {
    return true;
  }

  public Class getValueType() {
    return Date.class;
  }

  public String getBehaviourDocumentation() {
    return "Interprets the text as a date in the format \"yyyyDDD\" (Year and day of year, e.g. 2004032 is February 1st 2004) as specified by the Java platform java.text.SimpleDateFormat. '00000000' and a text value of all blanks is interpreted as null.";
  }

  public String getConfigurationDocumentation() {
    return "None.";
  }

}
