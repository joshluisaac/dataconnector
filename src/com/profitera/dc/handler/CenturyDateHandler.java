package com.profitera.dc.handler;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class CenturyDateHandler extends DefaultDateHandler{

  private Date minDate;
  private String compareField;
  
  public void configure(String args){
    minDate = null;
    compareField = null;
    if(args!=null){
      String[] split = args.split("[,]");
      super.configure(split[0]);
      if(split.length>1){
        try{
          minDate = formatter.parse(split[1]);
        }catch(ParseException e){
          log.warn(e.getMessage()+" , I assume this is another date field to compare");
          compareField = split[1];
        }
      }
    }
  }
  
  public Object getValue(String fieldText, Map allFields, IFieldTextHandlerContext context){
    Date date = (Date)super.getValue(fieldText, allFields, context);
    if(date==null){
      return date;
    }
    if(minDate!=null){
      if(date.before(minDate)){
        date = add100Years(date);
      }
    }else{
      if(compareField!=null){
        String anotherFieldText = (String)allFields.get(compareField);
        Date anotherDate = (Date)super.getValue(anotherFieldText, allFields, context);
        if(anotherDate!=null){
          if(date.before(anotherDate)){
            date = add100Years(date);
          }
        }
      }
    }
    return date;
  }
  
  private Date add100Years(Date date){
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.add(Calendar.YEAR, 100);
    return c.getTime();
  }
  
  public boolean isReversalSupported(){
    return false;
  }
  
  public String getBehaviourDocumentation() {
    return 
    "This Handler behave just like the DefaultDateHandler but it provided functionality where \n" +
    "it can parse both dates and adjust the date based on the other one, adding 100 years if the \n" +
    "date is below the other date specified. Just add another argument in the configuration parameter \n" +
    "after the date format separate by comma, it can be a static minimum date or another date field \n" +
    "name in the same file. No reversal supported.";
  }
  
  public String getConfigurationDocumentation() {
    return "Date format in string, and another date field's name or static minimum date delimited by comma. \n"+"" +
    "Supported date formats are:" +
    "<screen>\n" 
          + "\n" + "y   Year      Year    1996; 96"
          + "\n" + "M   Month in year   Month   July; Jul; 07"
          + "\n" + "D   Day in year   Number  189"
          + "\n" + "d   Day in month    Number  10"
          + "\n" + "a   Am/pm marker    Text  PM"
          + "\n" + "H   Hour in day (0-23)  Number  0"
          + "\n" + "k   Hour in day (1-24)  Number  24"
          + "\n" + "K   Hour in am/pm (0-11)  Number  0"
          + "\n" + "h   Hour in am/pm (1-12)  Number  12"
          + "\n" + "m   Minute in hour    Number  30"
          + "\n" + "s   Second in minute  Number  55"
          + "\n" + "S   Millisecond     Number  978"
          + "\n" + "</screen>";
  }
  
}
