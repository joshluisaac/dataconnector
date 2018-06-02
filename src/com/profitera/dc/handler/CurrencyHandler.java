/*
 * Created on Mar 9, 2005
 */
package com.profitera.dc.handler;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Map;

import com.profitera.util.Strings;

public class CurrencyHandler extends AbstractFieldTextHandler {
	
	public static final String SPACE = "space";
	private String posSign = " ";

  int impliedDecimalLocation = 2;
  int divisor;

  /* (non-Javadoc)
   * @see com.profitera.dc.IFieldTextHandler#configure(java.lang.String)
   */
  public void configure(String args) {
    if (args != null && args.trim().length() != 0){
    	int decimalIdx = args.indexOf("decimal=");
    	int signIdx = args.indexOf("possign=");
    	if (decimalIdx > -1){
    		if (signIdx > -1){
    			impliedDecimalLocation = Integer.parseInt(args.substring(args.indexOf('=') + 1, signIdx));
    			String signKey = args.substring(signIdx, args.length());
    			String[] signs = signKey.split("=");
    			if (signs.length == 2 && signs[1].trim().length() > 0){
    				if (signs[1].trim().equalsIgnoreCase(SPACE))
    					posSign = SPACE;
    				else
    					posSign = signs[1].trim();
    			}
    		} else {
    			impliedDecimalLocation = Integer.parseInt(args.substring(args.indexOf('=') + 1, args.length()));
    		}
    	} else {
    			impliedDecimalLocation = Integer.parseInt(args);
    	}
    }

    divisor = 1;
    for(int i = 0; i<impliedDecimalLocation;i++)
      divisor = divisor * 10;
  }

  /* (non-Javadoc)
   * @see com.profitera.dc.IFieldTextHandler#getValue(java.lang.String)
   */
  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context) {
    if (fieldText == null || "".equals(fieldText)) {
      return null;
    }
    fieldText = trim(fieldText);
    try{
    	if (impliedDecimalLocation == 0) {
    		return new BigDecimal(fieldText);
    	}
    	long l = Long.parseLong(fieldText);
    	boolean isNegative = l < 0;
    	long dollars = getDollars(l);
    	long cents = getCents(l);
    	String paddedCents = Strings.leftPad("" + cents, impliedDecimalLocation, '0');
    	String temp = dollars+"";
    	if (isNegative && dollars >= 0){
    		temp = "-" + temp;
    	}
    	return new BigDecimal(temp + "." + paddedCents);
    }catch(NumberFormatException e){
    	log.warn("Parsing failed for value "+fieldText+". Invalid number format.", e);
    }
    return null;
  }

  protected long getCents(long l) {
    long cents = l%divisor;
    if (cents < 0)
      cents *= -1;
    String padCent = Long.toString(cents);
    return Long.parseLong(padCent);
  }

  /**
   * @param l
   * @return
   */
  protected long getDollars(long l) {
    long dollars = l/divisor;
    return dollars;
  }
  
  public String getReverseValue(Object value, Map allFields, String defaultValue, int length){
  	if (value == null){
  		if (defaultValue != null && defaultValue.length() != 0)
  			return defaultValue;
  		else if (length > 0)
  			return posSign + Strings.leftPad("", length - 1, '0');
  		else
  			return null;
  	}
  	try{
  		Number castedValue = (Number)value;
  		if (length > 0) {
  	  	String format = Strings.pad("", (length - impliedDecimalLocation - 1), '0') + "." +  Strings.pad("", impliedDecimalLocation, '0');
  	  	DecimalFormat df = new DecimalFormat(format);
  	  	String formattedValue = df.format(castedValue);
  	  	String formattedValueWODecimal = formattedValue.substring(0, formattedValue.indexOf('.')) + formattedValue.substring(formattedValue.indexOf('.') + 1, formattedValue.length());
  	  	if (castedValue.doubleValue() > 0)
  	  		formattedValueWODecimal = posSign + formattedValueWODecimal;
  	  	return formattedValueWODecimal;
    	} else {
    		return castedValue.toString();
    	}
  	}catch(ClassCastException e){
  		log.warn("Parsing failed for value "+value+". Invalid number format.", e);
  	}
  	return null;
  	
  }
  
  protected String trim(String fieldText){
    fieldText = fieldText.replace('+', ' ').trim();
    if(fieldText.endsWith("-")){
    	fieldText = "-"+fieldText.replace('-', ' ').trim();
    }
    return fieldText;
  }

  /* (non-Javadoc)
   * @see com.profitera.dc.IFieldTextHandler#getValueType()
   */
  public Class getValueType() {
    return BigDecimal.class;
  }
  
  public boolean isReversalSupported(){
  	return true;
  }

  public String getBehaviourDocumentation() {
    return "Inserts an implied decimal at the location specified, returning a number.";
  }

  public String getConfigurationDocumentation() {
    return "An integer argument specifying the number of implied decimal places, defaults to 2 (100 becomes $1.00)";
  }
}