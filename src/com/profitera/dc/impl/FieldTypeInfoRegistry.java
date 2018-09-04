package com.profitera.dc.impl;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.profitera.dc.handler.CurrencySumHandler;
import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.util.Strings;
import com.profitera.util.reflect.Reflect;
import com.profitera.util.reflect.ReflectionException;

public class FieldTypeInfoRegistry {
  private static final String FULL_DATE_FORMAT = "yyyy-MM-dd-HH.mm.ss.SSSSSS";
  private static final int FULL_DATE_FORMAT_LENGTH = FULL_DATE_FORMAT.length();
  private static final String SQL_DATE_FORMAT = "yyyy-MM-dd";
  private static final int SQL_DATE_FORMAT_LENGTH = SQL_DATE_FORMAT.length();
  private final Map<Class<?>, IFieldTypeInfo> types = new HashMap<Class<?>, IFieldTypeInfo>();
  public FieldTypeInfoRegistry (final LoadDefinition definition) {
    types.put(String.class, new IFieldTypeInfo(){
      @Override
      public String getDatabaseColumnDefinition(FieldDefinition field, int width) {
        return "" + field + " VARCHAR(" + width + ")";
      }
      @Override
      public int getFieldTextWidth(FieldDefinition field, IFieldTextHandler handler) {
        return field.getFieldLengthInt();
      }
      @Override
      public String render(Object value, int width) {
        return Strings.pad(value.toString(), width);
      }});
    types.put(Integer.class, new IFieldTypeInfo(){
      @Override
      public String getDatabaseColumnDefinition(FieldDefinition field, int width) {
        return "" + field + " BIGINT";
      }
      @Override
      public int getFieldTextWidth(FieldDefinition field, IFieldTextHandler handler) {
        return getIntegerWidth();
      }
      @Override
      public String render(Object value, int width) {
        return Strings.leftPad(value.toString(), width, '0');
      }});
    types.put(Boolean.class, new IFieldTypeInfo(){
      @Override
      public String getDatabaseColumnDefinition(FieldDefinition field, int width) {
        return "" + field + " SMALLINT";
      }
      @Override
      public int getFieldTextWidth(FieldDefinition field, IFieldTextHandler handler) {
        return 1;
      }
      @Override
      public String render(Object value, int width) {
        // Width here will always be 1
        if (((Boolean)value)) {
          return "1";
        } else {
          return "0";
        }
      }});
    types.put(Long.class, new IFieldTypeInfo(){
      @Override
      public String getDatabaseColumnDefinition(FieldDefinition field, int width) {
        return "" + field.getFieldName() + " BIGINT";
      }
      @Override
      public int getFieldTextWidth(FieldDefinition field, IFieldTextHandler handler) {
        return 20; // Long.Max_value # digits
      }
      @Override
      public String render(Object value, int width) {
        return Strings.leftPad(value.toString(), width, '0');
      }});
    types.put(Date.class, new IFieldTypeInfo(){
      @Override
      public String getDatabaseColumnDefinition(FieldDefinition field, int width) {
        return "" + field.getFieldName() + " TIMESTAMP";
      }
      @Override
      public int getFieldTextWidth(FieldDefinition field, IFieldTextHandler handler) {
        return FULL_DATE_FORMAT_LENGTH;
      }
      @Override
      public String render(Object value, int width) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(FULL_DATE_FORMAT);
        return dateFormat.format((Date) value);
      }});
    types.put(java.sql.Date.class, new IFieldTypeInfo(){
      @Override
      public String getDatabaseColumnDefinition(FieldDefinition field, int width) {
        return "" + field.getFieldName() + " DATE";
      }
      @Override
      public int getFieldTextWidth(FieldDefinition field, IFieldTextHandler handler) {
        return SQL_DATE_FORMAT_LENGTH;
      }
      @Override
      public String render(Object value, int width) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(SQL_DATE_FORMAT);
        return dateFormat.format((Date) value);
      }});
    types.put(BigDecimal.class, new IFieldTypeInfo(){
      @Override
      public String getDatabaseColumnDefinition(FieldDefinition field, int width) {
        return "" + field.getFieldName() + " DECIMAL(" + (width + 6) +", 6)";
      }
      @Override
      public int getFieldTextWidth(FieldDefinition field, IFieldTextHandler handler) {
        if (handler.getClass().equals(CurrencySumHandler.class)) {
          CurrencySumHandler h = (CurrencySumHandler) handler;
          String[] sumFieldNames = h.getSumFieldNames();
          FieldDefinition originalField = field;
          if (sumFieldNames.length > 0) {
            String[] fields = field.getHandlerArgs().split("[,]");
            field = definition.getField(fields[fields.length - 1]);
          }
          if (field == null) {
            throw new NullPointerException("Unable to parse configuration for CurrencySumHandler for " + originalField);
          }
        }
        // Add 1 for the insertion of a decimal place because original might be
        // implied decimal
        // and we prefer consistency over saving one byte per line.
        return field.getFieldLengthInt() + 1;
      }
      @Override
      public String render(Object value, int width) {
        // Add 1 for the possible insertion of a decimal place because
        // original might be implied decimal
        String format = Strings.pad("", width, '#');
        BigDecimal d = (BigDecimal) value;
        if (d.scale() > 0) {
          format = format.substring(0, format.length() - d.scale() - 1) + "."
              + format.substring(format.length() - d.scale());
        }
        DecimalFormat df = new DecimalFormat(format);
        return Strings.leftPad(df.format(d), width);
      }});
  }
  int getIntegerWidth() {
    return 14; // Integer.Max_value # digits
  }
  public IFieldTypeInfo getHandlerTypeInfo(IFieldTextHandler handler) {
    Class<?> valueType = handler.getValueType();
    return getTypeInfo(valueType);
  }
  public IFieldTypeInfo getTypeInfo(Class<?> valueType) {
    IFieldTypeInfo iFieldTypeInfo = types.get(valueType);
    if (iFieldTypeInfo == null) {
      throw new RuntimeException("Sorry, " + valueType.getName() + " not yet supported");
    }
    return iFieldTypeInfo;
  }
  public IFieldTypeInfo getHandlerTypeInfo(FieldDefinition d) throws ReflectionException {
    return getHandlerTypeInfo(getHandler(d));
  }
  public IFieldTextHandler getHandler(FieldDefinition d) throws ReflectionException {
    IFieldTextHandler handler = (IFieldTextHandler) Reflect.invokeConstructor(d.getHandler(), null, null);
    handler.configure(d.getHandlerArgs());
    return handler;
  }


}
