package com.profitera.dc.handler;

import java.util.Map;

public class StripPrefixAndAppendPhoneNumberHandler extends
    StripPrefixPhoneNumberHandler {

  private String prefix;

  public String getBehaviourDocumentation() {
    return "Behaves exactly as " + StripPrefixPhoneNumberHandler.class.getName() 
    + " but the first in the list of values to strip is treated as a required prefix. "
    + "Even if the number after stripping already has the prefix it is still added.";
  }

  public String getConfigurationDocumentation() {
    return "A required prefix followed by " + super.getConfigurationDocumentation();
  }


  public void configure(String args) {
    if (args != null){
      String[] temp = args.split("[;]", 2);
      if (temp.length > 0){
        prefix = temp[0];
        args = "";
      }
      if (temp.length > 1){
        args = temp[1];
      }
    }
    super.configure(args);
  }

  public Object getValue(String fieldText, Map allFields, String defaultValue, IFieldTextHandlerContext context) {
    String v = (String) super.getValue(fieldText, allFields, defaultValue, context);
    if (v == null || v.equals("")){
      return v;
    } else {
      return prefix + v;
    }
  }

}
