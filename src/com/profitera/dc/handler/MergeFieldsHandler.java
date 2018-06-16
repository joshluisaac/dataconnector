package com.profitera.dc.handler;

import java.util.Map;

import com.profitera.util.Strings;

/**
 * @author jambu
 */
public class MergeFieldsHandler implements IFieldTextHandler {

  String fields[] = null;
  String del = " ";
  /* (non-Javadoc)
   * @see com.profitera.dc.handler.IFieldTextHandler#configure(java.lang.String)
   */
  public void configure(String args) {
    if (args != null && args.trim().length() != 0){
    	int fieldIdx = args.indexOf("fields=");
    	int delIdx = args.indexOf("del=");
    	if (fieldIdx > -1){
    		if (delIdx > -1){
    			fields = args.substring(args.indexOf('=') + 1, delIdx).split(";");
    			String delKey = args.substring(delIdx, args.length());
    			String[] dels = delKey.split("=");
    			if (dels.length == 2)
    				del = dels[1];
    			else if (dels.length == 1)
    				del = "";
    		} else {
    			fields = args.substring(args.indexOf('=') + 1, args.length()).split(";");
    		}
    	} else {
    			fields = args.split(";");
    	}
    }
  }
  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    return getValue(fieldText, allFields, (String)null);
  }
  private Object getValue(String fieldText, Map allFields, String defaultValue) {
    if (fields == null || fields.length == 0) {
      return null;
    }
    String returnValue = new String();
    for (int i = 0; i < fields.length; i++){
    	if (allFields.get(fields[i]) == null) {
    		continue;
    	}
      String tmpStr = (String)allFields.get(fields[i]);
      returnValue += tmpStr + (i != fields.length - 1 ? del : "");
    }
    if (returnValue.length() == 0) {
      return defaultValue;
    }
    return returnValue;
  }

  /**
   * This is the only handler where the getReverseValue does the same manipulation
   * like its getValue method.
   * It's used when multiple field need to be merged into one text and placed
   * in a file.
   * @param value
   * @param allFields
   * @param defaultValue
   * @param length
   * @return
   */
  public String getReverseValue(Object value, Map allFields, String defaultValue, int length){
  	String text = (String)getValue(null, allFields, defaultValue);
  	if (text == null){
  		if (defaultValue != null && defaultValue.length() != 0)
  			return defaultValue;
  		else if (length > 0)
  			return " " + Strings.leftPad("", length - 1, '0');
  		else
  			return null;
  	}
  	
  	return (length > 0 ? Strings.pad(text, length) : text);
  }

  public boolean isReversalSupported(){
  	return true;
  }

  /* (non-Javadoc)
   * @see com.profitera.dc.handler.IFieldTextHandler#getValueType()
   */
  public Class getValueType() {
    return String.class;
  }

  public String getBehaviourDocumentation() {
    return "Returns text merged from multiple fields, this is useful when multiple comment, address lines or text fields need to be merged and inserted into single db field.";
  }

  public String getConfigurationDocumentation() {
    return "IMPORTANT! This handler must have arguments, else it will return null. Argument for this handler is the field name of the fields to be merged, separated by ';'. Example, to merge fields COMMENT and REMARK, argument for this handler is: COMMENT;REMARK";
  }
  @Override
  public Object getValue(String fieldText, Map<String, String> allFields, String defaultValue, IFieldTextHandlerContext context) {
    Object result = getValue(fieldText, allFields, context);
    if (result != null) {
      return result;
    } else {
      return defaultValue;
    }
  }
}