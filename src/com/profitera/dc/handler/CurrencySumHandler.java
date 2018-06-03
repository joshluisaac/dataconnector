/*
 * Created on Mar 9, 2005
 */
package com.profitera.dc.handler;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author jamison
 */
public class CurrencySumHandler extends CurrencyHandler {
  private String[] otherFieldNames;
  private String[] fieldValues;

  public void configure(String args) {
    if (args == null || args.equals("")){
      super.configure(args);
      otherFieldNames = new String[0];
    } else {
      String[] split = args.split("[,]");
      try {
        Integer.parseInt(split[0]);
        super.configure(split[0]);
        split = args.substring(args.indexOf(',')).split("[,]");
      } catch (NumberFormatException e){
        super.configure(null);
      }
      otherFieldNames = split;
      fieldValues = new String[split.length + 1];
    }
  }
  public String[] getSumFieldNames() {
    return otherFieldNames;
  }
  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (otherFieldNames.length == 0) {
      return super.getValue(fieldText, allFields, context);
    }
    for (int i = 0; i < otherFieldNames.length; i++) {
      fieldValues[i] = (String) allFields.get(otherFieldNames[i]);    
    }
    fieldValues[otherFieldNames.length] = fieldText;
    boolean allNull = true;
    BigDecimal d = BigDecimal.ZERO;
    for (int i = 0; i < fieldValues.length; i++) {
      String f = fieldValues[i];
      if (f == null || "".equals(f) ) {
        continue;
      }
      Object assignedFieldValue = super.getValue(f, allFields, context);
      if (assignedFieldValue != null) {
        d = d.add((BigDecimal) assignedFieldValue);
      }
      allNull = false;
    }
    if (allNull) {
      return super.getValue(null, allFields, context);
    }
    return d;
  }

  public String getReverseValue(Object value, String defaultValue, int length){
  	throw new RuntimeException(this.getClass().getName() + " do not support reversal!");
  }

  public boolean isReversalSupported(){
  	return false;
  }
  public String getBehaviourDocumentation() {
    return "Behaves as CurrencyHandler does, but sums a specified set of fields after interpreting each individually.";
  }
  public String getConfigurationDocumentation() {
    return "If the first argument is an integer it is interpreted as the implied decimal location, the remaining items delimited by commas are used as the field names for the fields to sum.";
  }
  
  
}