/*
 * Created on May 4, 2005
 */
package com.profitera.dc.handler;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.profitera.util.Strings;

public class DefaultDateHandler extends AbstractFieldTextHandler {
  SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

  private static final char[] LITERAL_FORMATS_IDENTIFIERS = {'-', ',', '/', ' ', ':', '.'};
  private static final int CHAR_FORMAT = 1;
  private static final int NUM_FORMAT = 2;
  /* (non-Javadoc)
   * @see com.profitera.dc.handler.IFieldTextHandler#configure(java.lang.String)
   */
  public void configure(String args) {
    if (args != null && args.trim().length() != 0)
      formatter = new SimpleDateFormat(args);
  }

  /* (non-Javadoc)
   * @see com.profitera.dc.handler.IFieldTextHandler#getValue(java.lang.String, java.util.Map)
   */
  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText != null && fieldText.equalsIgnoreCase("CURRENT_DATE")) {
      return new Date();
    }
    int fmtLength = formatter.toPattern().length();
    String nullDateString = "";
    for (int i = 0; i < fmtLength; i++) {
      nullDateString += "0";
    }
    if (fieldText == null || fieldText.trim().length() == 0 || fieldText.indexOf(nullDateString) != -1) {
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
      return formatter.parse(fieldText);
    } catch (ParseException e) {
    	log.warn("Parsing failed for date value " + fieldText + " with pattern " + formatter.toPattern());
    	log.warn("Error is: " + e.getMessage());
    	return null;
    }
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length){
  	if (value == null){
  		if (defaultValue != null && defaultValue.length() != 0)
  			return defaultValue;
  		else if (length > 0)
  			return formatReverseValue("", length, formatType(formatter));
  		else
  			return null;
  	}
  	String formattedDate = formatter.format((Date)value);
  	return (length > 0 ? formatReverseValue(formattedDate, length, formatType(formatter)) : formattedDate);
  }

  public boolean isReversalSupported(){
  	return true;
  }

  /* (non-Javadoc)
   * @see com.profitera.dc.handler.IFieldTextHandler#getValueType()
   */
  public Class getValueType() {
    return Date.class;
  }
  
  private int formatType(SimpleDateFormat fmt){
  	for (int i = 0; i < LITERAL_FORMATS_IDENTIFIERS.length; i++)
  		if (fmt.toPattern().indexOf(LITERAL_FORMATS_IDENTIFIERS[i]) > -1)
  			return CHAR_FORMAT;
  	return NUM_FORMAT;
  }
  
  private String formatReverseValue(String value, int length, int fmtType){
  	if (fmtType == DefaultDateHandler.CHAR_FORMAT)
  		return Strings.pad(value, length);
  	return Strings.leftPad(value, length, '0');
  }

  public String getBehaviourDocumentation() {
    return " Interprets the text as a date in the format \"yyyyMMdd\" as specified by the Java platform java.text.SimpleDateFormat. '00000000' and a text value of all blanks is interpreted as null."
    +"<note><para>" + "If defaultValue is specified for this handler, and the defaultValue is the reserved keyword \"CURRENT_DATE\" (case sensitive), this handler will return the current date in the event where the location in the file for this field is null or empty (as in cases where the location is specified as 0-0). This is useful for mapping current system date into database field." + "</para></note>";
  }

  public String getConfigurationDocumentation() {
    return "Date format in string. Supported date formats are:" +
    "<screen>\n" 
          + "\n" + "y   Year      Year    1996; 96"
          + "\n" + "M   Month in year   Month   July; Jul; 07"
          + "\n" + "D   Day in year   Number  189"
          + "\n" + "d   Day in month    Number  10"
          + "\n" + "a   Am/pm marker    Text  PM"
          + "\n" + "H   Hour in day (0-23)  Number  0"
          + "\n" + "k   Hour in day (1-24)  Number  24"
          + "\n" + "K   Hour in am/pm (0-11)  Number  0"
          + "\n" + "h   Hour in am/pm (1-12)  Number  12"
          + "\n" + "m   Minute in hour    Number  30"
          + "\n" + "s   Second in minute  Number  55"
          + "\n" + "S   Millisecond     Number  978"
          + "\n" + "</screen>";
  }
  
  protected SimpleDateFormat getFormatter(){
    return formatter;
  }
}