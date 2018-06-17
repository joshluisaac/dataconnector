/*
 * Created on Mar 9, 2005
 */
package com.profitera.dc.handler;

import java.util.Map;

import com.profitera.util.Strings;

public class PassThroughHandler extends AbstractFieldTextHandler {

  /**
   * This default handler turns the empty String into null
   * 
   * @see com.profitera.dc.handler.IFieldTextHandler#getValue(java.lang.String,
   *      Map)
   */
  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText != null && fieldText.length() == 0)
      return null;
    return fieldText;
  }

  public void configure(String args) {
    // Nothing to configure since I don't do anything
  }

  public String getReverseValue(Object value, Map allFields, String defaultValue, int length) {
    if (value == null) {
      if (defaultValue != null && defaultValue.length() != 0) {
        return defaultValue;
      } else if (length > 0) {
        return Strings.pad("", length);
      } else {
        return null;
      }
    }
    return (length > 0 ? Strings.pad((String) value, length) : (String) value);
  }

  public boolean isReversalSupported() {
    return true;
  }

  public Class getValueType() {
    return String.class;
  }

  public String getBehaviourDocumentation() {
    return "Returns the text value of the field, it essentially does nothing. Using this handler explicitly is pointless as it is the handler applied when none is specified to load text.";
  }

  public String getConfigurationDocumentation() {
    return "None.";
  }

}
