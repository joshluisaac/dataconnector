package com.profitera.dc.handler;

import java.util.Map;

import com.profitera.util.Strings;

public class RunningNumberHandler extends AbstractFieldTextHandler {

  private long start = 0;
  private int initialLength = 0;

  /*
   * (non-Javadoc)
   * 
   * @see com.profitera.dc.handler.IFieldTextHandler#configure(java.lang.String)
   */
  public void configure(String args) {
    if (args != null && args.trim().length() != 0)
      try {
        initialLength = Integer.parseInt(args.trim());
      } catch (Exception e) {
        initialLength = 0;
      }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.profitera.dc.handler.IFieldTextHandler#getValue(java.lang.String,
   * java.util.Map)
   */
  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    start++;
    return new Long(start);
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    start++;
    if (length > 0) {
      return Strings.leftPad(Long.toString(start), length, '0');
    } else if (initialLength > 0) {
      return Strings.leftPad(Long.toString(start), initialLength, '0');
    } else {
      return Long.toString(start);
    }
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
    return Long.class;
  }

  public String getBehaviourDocumentation() {
    return "Produces a running number.";
  }

  public String getConfigurationDocumentation() {
    return "An integer to determine total length of the running number.";
  }
}
