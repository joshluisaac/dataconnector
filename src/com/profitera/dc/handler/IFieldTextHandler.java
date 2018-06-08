package com.profitera.dc.handler;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author jamison
 */
public interface IFieldTextHandler {
  public static final Log log = LogFactory.getLog(IFieldTextHandler.class);
	
  public void configure(String args);
  public Object getValue(String fieldText, Map<String, String> allFields, String defaultValue, IFieldTextHandlerContext context);
  public String getReverseValue(Object value, Map<String, Object> allFields, String defaultValue, int length);
  public boolean isReversalSupported();
  public Class<?> getValueType();
  public String getBehaviourDocumentation();
  public String getConfigurationDocumentation();
}
