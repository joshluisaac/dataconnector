package com.profitera.dc.handler;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.profitera.util.Strings;

public class ShortDateHandler extends AbstractFieldTextHandler {
  private SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
  public void configure(String args) {
    if (args != null && args.trim().length() != 0)
      formatter = new SimpleDateFormat(args);
  }

  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText != null && fieldText.equalsIgnoreCase("CURRENT_DATE")) {
      return new Date();
    }
    String nullDateString = new String();
    int fmtLength = formatter.toPattern().length();
    for (int i = 0; i < fmtLength; i++) {
      nullDateString += "0";
    }
    if (fieldText == null || fieldText.trim().length() == 0 || fieldText.indexOf(nullDateString) != -1) {
      return null;
    }
    int patchLength = fmtLength - fieldText.length();
    for (int i = 0; i < patchLength; i += 2) {
      fieldText += "01";
    }
    return parseDate(fieldText);
  }

  private Date parseDate(String fieldText) {
    formatter.setLenient(false);
    try {
      return formatter.parse(fieldText);
    } catch (ParseException e) {
      log.warn("Parsing failed for data value " + fieldText + " with pattern " + formatter.toPattern());
      log.warn("Error is: " + e.getMessage());
      return null;
    }
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
    return formatter.format((Date) value);
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
    return "accepts date which comes in 'yyMM' format in the hand off file. It will then appends the 1st of the month to the date before parsing it in.";
  }

  public String getConfigurationDocumentation() {
    return "None.";
  }
}
