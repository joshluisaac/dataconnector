/*
 * Created on Mar 9, 2005
 */
package com.profitera.dc.handler;

import java.util.Map;

public class IntegerDivisionHandler extends AbstractFieldTextHandler {

  int divisor;

  public void configure(String args) {
    if (args == null || args.equals(""))
      divisor = 1;
    else
      divisor = Integer.parseInt(args);
    if (divisor == 0)
      throw new RuntimeException(
          "IntegerDivisionHandler will not accept zero ('0') as handler config.\nA number cannot be divided by zero");
  }

  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText == null || "".equals(fieldText))
      return null;
    int value = Integer.parseInt(fieldText);
    if (value == 0)
      return new Integer(value);
    return new Integer(value / divisor);
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    throw new RuntimeException(this.getClass().getName() + " does not support reversal!");
  }

  public boolean isReversalSupported() {
    return false;
  }

  public Class getValueType() {
    return Double.class;
  }

  public String getBehaviourDocumentation() {
    return "Return a round which is the result of another round be divided by the number passed in as handler config. If handler config is not specified, 1 will be used as default.";
  }

  public String getConfigurationDocumentation() {
    return "Argument is the number to be divided with. Default value is 1.";
  }
}
