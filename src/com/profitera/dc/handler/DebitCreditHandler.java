/*
 * Created on May 5, 2005
 */
package com.profitera.dc.handler;

import java.util.Map;

import com.profitera.util.Strings;

public class DebitCreditHandler extends AbstractFieldTextHandler {

  public static final String DEBIT = "D";
  public static final String CREDIT = "C";

  public void configure(String args) {
  }

  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText == null || fieldText.trim().length() == 0 || !fieldText.trim().equalsIgnoreCase(CREDIT))
      return new Integer(1);
    return new Integer(-1);
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    if (value == null) {
      if (defaultValue != null && defaultValue.length() != 0)
        return defaultValue;
      else if (length > 0)
        return Strings.leftPad("", length, ' ');
      else
        return null;
    }

    String code = null;
    if (((Integer) value).intValue() == -1)
      code = "D";
    else if (((Integer) value).intValue() == 1)
      code = "C";
    else
      code = "";
    if (length > 0)
      code = Strings.pad(code, length, ' ');

    return code;
  }

  public boolean isReversalSupported() {
    return true;
  }
  public Class getValueType() {
    return Integer.class;
  }

  public String getBehaviourDocumentation() {
    return "Interprets 'C' as credit and anything else as debit, credit is returned a 1 and debit as -1.";
  }

  public String getConfigurationDocumentation() {
    return "None.";
  }
}
