package com.profitera.dc.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.profitera.util.Strings;

public class StripPrefixPhoneNumberHandler implements IFieldTextHandler {

  private String[] disallowed;

  public void configure(String args) {
    if (args == null){
      disallowed = new String[0];
    } else {
      disallowed = args.split("[;]");
      List l = new ArrayList(Arrays.asList(disallowed));
      for (Iterator iter = l.iterator(); iter.hasNext();) {
        String element = (String) iter.next();
        if (element == null || element.trim().length() == 0){
          iter.remove();
        }
      }
      disallowed = (String[]) l.toArray(new String[0]);
    }
  }

  public String getBehaviourDocumentation() {
    return "Finds the largest block of all-numeric text in the value passed to the handler and considers it the phone number. The process then checks the start of that number for any disallowed prefixes, processing then in the order they are configured (even if one matches the others are still tested against the number).";
  }

  public String getConfigurationDocumentation() {
    return "A semi-colon delimited list of disallowed number prefixes";
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    String v = (String) getValue(value == null ? null : value.toString(), allFields, defaultValue, null);
    if (v == null){
      return null;
    }
    return length == -1 ? v : Strings.pad(v, length).trim();
  }


  public Object getValue(String fieldText, Map allFields, String defaultValue, IFieldTextHandlerContext context) {
    if (fieldText == null) return defaultValue;
    String number = getNumber(fieldText);
    if (number == null) return null;
    for (int i = 0; i < disallowed.length; i++) {
      if (number.startsWith(disallowed[i])){
        if (disallowed[i].length() == number.length()){
          // I choose blank instead of null b/c of the subtle difference in that there
          // _is_ a number, its just completely made up of an invalid prefix
          number = "";
          break;
        } else {
          number = number.substring(disallowed[i].length());
        }
      }
    }
    return number;
  }

  public Class getValueType() {
    return String.class;
  }

  public boolean isReversalSupported() {
    return true;
  }
  
  /**
   * This is a copy and paste job of SMSFileFormatProcessor's getMobileNumber,
   * I would like to reuse the code but since they are targetting 2 different outputs
   * and the SMS processor is not actually part of the dataconnector I think it
   * is better to just let them evolve separately.
   * @param rawNumber
   * @return
   */
  private String getNumber(String rawNumber) {
    if (rawNumber == null) {
      return null;
    }
    String[] segments = rawNumber.replaceAll("\\W","").split("\\D+");
    if (segments.length == 0){
      log.warn("Unable to find mobile number in stored value of " + rawNumber);
      return null;
    }
    int maxLen = 0;
    int biggest = 0;
    for (int i = 0; i < segments.length; i++) {
        if (segments[i].length() > maxLen){
          biggest = i;
          maxLen = segments[i].length();
        }
      }
    String val = segments[biggest];
    if (maxLen < rawNumber.length()){
      log.debug("Provided number '" + rawNumber + "' will be interpreted as number '" + val + "'");
    }
    return val;
  }

}
