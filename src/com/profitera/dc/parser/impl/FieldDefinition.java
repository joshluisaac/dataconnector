package com.profitera.dc.parser.impl;

import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.handler.LongHandler;
import com.profitera.dc.handler.PassThroughHandler;
import com.profitera.util.Strings;

public class FieldDefinition {

  private final String fieldName;
  private final boolean key;
  private Location location;
  private String defaultValue;
  private String handler;
  private String handlerArgs;
  private String[] filterOnValue;
  private String[] filterOnField;
  private String[] nullDefinition;
  private boolean external = false;
  private LookupDefinition lookupDefinition;
  // Default this to true because properties parser does not set it
  private boolean isOptional = true;
  private String remarks = "";

  public FieldDefinition(String fieldName, Location l, boolean isKey) {
    if (fieldName != null) {
      fieldName = fieldName.trim();
    }
    this.fieldName = Strings.nullifyIfBlank(fieldName);
    if (l == null) {
      l = new Location(null, null);
    }
    this.location = l;
    this.key = isKey;
  }
  public void setLocation(Location l) {
    this.location = l;
    if (location == null) {
      location = new Location(null, null);
    }
  }
  public String getFieldName() {
    return fieldName;
  }
  public String getDefaultValue() {
    return defaultValue;
  }

  public boolean isKey() {
    return key;
  }

  public String getHandler() {
    if (handler == null) {
      return lookupDefinition == null ? PassThroughHandler.class.getName() : LongHandler.class.getName();
    }
    return handler;
  }

  public String getHandlerArgs() {
    return handlerArgs;
  }

  public String[] getFilterOnValue() {
    if (filterOnValue == null)
      return new String[0];
    return filterOnValue;
  }

  public String[] getFilterOnField() {
    if (filterOnField == null) {
      return new String[0];
    }
    return filterOnField;
  }

  public String[] getNullDefinition() {
    if (nullDefinition == null) {
      return new String[0];
    }
    return nullDefinition;
  }

  public boolean isExternal() {
    return external;
  }

  public LookupDefinition getLookupDefinition() {
    return lookupDefinition;
  }
  
  public boolean isLookupField() {
    return getLookupDefinition() != null;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = Strings.nullifyIfBlank(defaultValue);
  }

  public void setHandler(String handlerClass, String config) {
    handlerClass = Strings.nullifyIfBlank(handlerClass);
    if (handlerClass == null) {
      handler = null;
      handlerArgs = null;
      return;
    }
    handlerClass = handlerClass.trim();
    try {
      Class<?> c = Class.forName(handlerClass);
      if (c.isInterface()) {
        return;
      }
      c.asSubclass(IFieldTextHandler.class);
      handler = handlerClass;
      handlerArgs = config;
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Unable to set handler for field " + getFieldName() + " to " + handlerClass, e);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Unable to set handler for field " + getFieldName() + " to " + handlerClass, e);
    }
  }

  public void setFilterOnValue(String[] filterOnValue) {
    this.filterOnValue = Strings.asEmptyStringArrayIfNull(filterOnValue);
  }

  public void setFilterOnField(String[] filterOnField) {
    this.filterOnField = Strings.asEmptyStringArrayIfNull(filterOnField);
  }

  public void setNullDefinition(String[] nullDefinition) {
    this.nullDefinition = Strings.asEmptyStringArrayIfNull(nullDefinition);
  }

  public void setExternal(boolean external) {
    this.external = external;
  }

  public void setLookupDefinition(LookupDefinition lookupDefinition) {
    this.lookupDefinition = lookupDefinition;
  }

  public Location getLocation() {
    return location;
  }

  public String toString() {
    return getFieldName();
  }

  public void setOptional(boolean b) {
    this.isOptional = b;
  }

  public boolean isOptional() {
    return isOptional;
  }

  public int getFieldLengthInt() {
    int start = Integer.parseInt(getLocation().getStart());
    int end = Integer.parseInt(getLocation().getEnd());
    return end - start + 1;
  }

  public String getRemarks() {
    return remarks;
  }

  public void setRemarks(String remarks) {
    if (remarks == null) {
      remarks = "";
    }
    this.remarks = remarks;
  }
}
