package com.profitera.dc.handler;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import com.profitera.util.Strings;

public class DateHandler extends AbstractFieldTextHandler {

  SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

  private static final char[] LITERAL_FORMATS_IDENTIFIERS = {'-', ',', '/', ' ', ':', '.'};

  private static final int CHAR_FORMAT = 1;
  private static final int NUM_FORMAT = 2;

  @Override
  public void configure(String args) {
    if (args != null && args.trim().length() != 0)
      formatter = new SimpleDateFormat(args);
  }

  @Override
  public Object getValue(String fieldText, Map<String, String> allFields, IFieldTextHandlerContext context) {
    if (fieldText != null && fieldText.equalsIgnoreCase("CURRENT_DATE")) {
      return new java.sql.Date(System.currentTimeMillis());
    }
    int fmtLength = formatter.toPattern().length();
    String nullDateString = new String();
    for (int i = 0; i < fmtLength; i++) {
      nullDateString += "0";
    }
    if (fieldText == null || fieldText.trim().length() == 0 || fieldText.indexOf(nullDateString) != -1) {
      return null;
    }
    return parseDate(fieldText, context);
  }

  /**
   * @param fieldText
   * @param context 
   * @return
   */
  private java.sql.Date parseDate(String fieldText, IFieldTextHandlerContext context) {
    formatter.setLenient(false);
    try {
      java.util.Date d = formatter.parse(fieldText);
      return new java.sql.Date(d.getTime());
    } catch (ParseException e) {
      log.warn("Parsing failed for date value " + fieldText + " for " + context.getCurrentFieldName() + " with pattern " + formatter.toPattern() + " for line " + context.getCurrentLineNumber() + " of " + context.getLoadName());
      log.warn("Error is: " + e.getMessage());
      return null;
    }
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    if (value == null) {
      if (defaultValue != null && defaultValue.length() != 0) {
        return defaultValue;
      } else if (length > 0) {
        return formatReverseValue("", length, formatType(formatter));
      } else {
        return null;
      }
    }
    String formattedDate = formatter.format((java.util.Date) value);
    return (length > 0 ? formatReverseValue(formattedDate, length, formatType(formatter)) : formattedDate);
  }

  public boolean isReversalSupported() {
    return true;
  }

  public Class<?> getValueType() {
    return java.sql.Date.class;
  }

  private int formatType(SimpleDateFormat fmt) {
    for (int i = 0; i < LITERAL_FORMATS_IDENTIFIERS.length; i++)
      if (fmt.toPattern().indexOf(LITERAL_FORMATS_IDENTIFIERS[i]) > -1)
        return CHAR_FORMAT;
    return NUM_FORMAT;
  }

  private String formatReverseValue(String value, int length, int fmtType) {
    if (fmtType == DateHandler.CHAR_FORMAT)
      return Strings.pad(value, length);
    return Strings.leftPad(value, length, '0');
  }

  public String getBehaviourDocumentation() {
    return " Interprets the text as a date in the format \"yyyyMMdd\" as specified by the Java platform java.text.SimpleDateFormat. '00000000' and a text value of all blanks is interpreted as null."
        + "<note><para>"
        + "If defaultValue is specified for this handler, and the defaultValue is the reserved keyword \"CURRENT_DATE\" (case sensitive), this handler will return the current date in the event where the location in the file for this field is null or empty (as in cases where the location is specified as 0-0). This is useful for mapping current system date into database field."
        + "</para></note>";
  }

  public String getConfigurationDocumentation() {
    return "Date format in string. Supported date formats are:" + "<screen>\n" + "\n" + "y   Year      Year    1996; 96" + "\n"
        + "M   Month in year   Month   July; Jul; 07" + "\n" + "D   Day in year   Number  189" + "\n"
        + "d   Day in month    Number  10" + "\n" + "a   Am/pm marker    Text  PM" + "\n" + "H   Hour in day (0-23)  Number  0"
        + "\n" + "k   Hour in day (1-24)  Number  24" + "\n" + "K   Hour in am/pm (0-11)  Number  0" + "\n"
        + "h   Hour in am/pm (1-12)  Number  12" + "\n" + "m   Minute in hour    Number  30" + "\n"
        + "s   Second in minute  Number  55" + "\n" + "S   Millisecond     Number  978" + "\n" + "</screen>";
  }

  protected SimpleDateFormat getFormatter() {
    return formatter;
  }
}
