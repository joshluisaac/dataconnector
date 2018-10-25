package com.profitera.dc.parser;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.profitera.dc.InvalidLookupQueryException;
import com.profitera.dc.InvalidRefreshDataQueryException;
import com.profitera.dc.RecordDispenserFactory;
import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.handler.PassThroughHandler;
import com.profitera.util.Strings;
import com.profitera.util.Utilities;
import com.profitera.util.reflect.Reflect;
import com.profitera.util.reflect.ReflectionException;

public class ExtractDefinitionParser {

	private static final Log log = LogFactory.getLog(ExtractDefinitionParser.class);
	
	private static final String CONTROL_FILE_PREFIX = "controlFilePrefix";
  private static final String CONTROL_FILE_SUFFIX = "controlFileSuffix";
  public static final String SPACE = "SPACE";
	public static final String TAB = "TAB";

	private static final String OUT_TYPE_KEY = "outputType";
  private static final String DELIMITER = "delimiter";
	public static final String XML_NONE_LOCATION = "NONE";
	public static final String XML_ROOT_TAG = "rootTag";
	public static final String XML_START_TAG = "recordIndicatorTag";

  private static final String TEMPLATE_FOOTER_PROP_NAME = "footer";
  private static final String TEMPLATE_HEADER_PROP_NAME = "header";

  private static final String SELECT_QUERY_WHERE_KEY = "selectQueryWhere";
  private static final String IS_OPTIONAL_SUFFIX = "_isOptional";
  private static final String IS_EXTERNAL_SUFFIX = "_isExternal";
  private static final String DEFAULT_SUFFIX = "_default";
  private static final String LOOKUP_QUERY_SUFFIX = "_lookupQuery";
  private static final String LOOKUP_DATA_TYPE = "_lookupType";
  private static final String LOCATION_SUFFIX = "_location";
  private static final String HANDLER_SUFFIX = "_handler";
  private static final String HANDLER_ARGS_SUFFIX = "_handlerConfig";
  private static final String FILTER_ON_SUFFIX = "_filterOn";
  private static final String NULL_DEFINITION_SUFFIX = "_nullDefinition";
  private static final String MAP_IMPL = "java.util.HashMap";

  protected static final String OUTPUT_HEADER = "header";
  protected static final String OUTPUT_FOOTER = "footer";
  
  public final static int ASCENDING = 1;
  public final static int DESCENDING = -1;

  private String header;
  private String footer;
  protected String outputType = RecordDispenserFactory.FIXED_WIDTH;
  protected String delimiter;
  protected String xmlRootTag;
  protected String xmlStartTag;
  protected List fields;
  protected List lookupQueries;
  protected List lookupTypes;
  protected List defaultValues;
  protected List lookupQueryParams;
  protected String[] startIndexes;
  protected String[] endIndexes;
  protected String selectQueryWhere;
  protected boolean[] isExternal;
  protected boolean[] lookupOptional;
  protected boolean[] lookupMultipleParam;
  protected Class[] handlerClasses;
  protected String[] handlerArgs;
  protected String[] filterOns;
  protected String[] nullDefinitions;
  
  protected int recordSize = 0;
  protected String controlFileSuffix;
  protected String controlFilePrefix;
  protected int objectUsage = 0;
  protected static final int MAXIMUM_USAGE = 1;
  
  protected String outputHeader;
  protected String outputFooter;
  
  public ExtractDefinitionParser(){
	  objectUsage = 0;
  }
  
  public String getOutputType(){
  	return outputType;
  }
  
  public String getDelimiter(){
  	if (delimiter.equalsIgnoreCase(SPACE))
  		return " ";
  	if (delimiter.equalsIgnoreCase(TAB))
  		return "\t";
  	return delimiter;
  }

  public String getXMLRootTag(){
  	return xmlRootTag;
  }

  public String getXMLStartTag(){
  	return xmlStartTag;
  }

  public String getHeader(){
  	return header;
  }
  
  public String getFooter(){
  	return footer;
  }
  
  public List getFields(){
    return new ArrayList(fields);
  }
  
  public String getStartIndex(int fieldId){
    return startIndexes[fieldId];
  }
  
  public String getEndIndex(int fieldId){
    return endIndexes[fieldId];
  }
  
  public int getFieldLength(int fieldId){
  	return isFixedWidthFormat() ? (Integer.parseInt(getEndIndex(fieldId)) - Integer.parseInt(getStartIndex(fieldId)) + 1) : -1;
  }

  public boolean isFixedWidthFormat() {
    return getOutputType().equalsIgnoreCase(RecordDispenserFactory.FIXED_WIDTH);
  }
  public int getRecordSize() {
    return recordSize;
  }

  public void writeQueriesToBuffer(final BufferedWriter xmlBuffer) throws IOException {
    writeQueries(xmlBuffer);
  }

  private void writeQueries(final BufferedWriter xmlBuffer) throws IOException {
    writeSelectQuery(xmlBuffer);
    writeLookupQueries(xmlBuffer);
  }

  private void writeLookupQueries(BufferedWriter xmlBuffer) throws IOException {
    for (int i = 0; i < lookupQueries.size(); i++) {
      if (lookupQueries.get(i) != null){
        writeLookupQuery(getLookupQueryName(i), (String) lookupQueries.get(i), (String)lookupTypes.get(i), xmlBuffer);
      }
    }
  }

  private void writeLookupQuery(String name, String sqltext, String dataType, BufferedWriter xmlBuffer) throws IOException{
    xmlBuffer.write("<select id=" + Strings.quoteString(name));
    xmlBuffer.write(" resultClass=" + Strings.quoteString(dataType));
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n  <![CDATA[\n");
    xmlBuffer.write(sqltext);
    xmlBuffer.write("\n  ]]>\n</select>\n");
  }

  private void writeSelectQuery(BufferedWriter xmlBuffer) throws IOException {
    xmlBuffer.write("<select id=" + Strings.quoteString(getSelectName()));
    xmlBuffer.write(" resultClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n <![CDATA[\n");
    xmlBuffer.write(selectQueryWhere == null ? "" : selectQueryWhere);
    xmlBuffer.write("\n  ]]>\n</select>\n");
  }

  public String getSelectName() {
    return "SELECT_QUERY";
  }

  public boolean hasLookupQuery(int index) {
    return lookupQueries.get(index) != null;
  }
  
  public boolean isReferencialLookupQuery(int index){
    if (isLookupWithMultipleParam(index))
      return true;
    List lookupFields = getLookupQueryParams(index);
    if (lookupFields.size() == 1){
      String param = (String)lookupFields.get(0);
      String field = (String)getFields().get(index);
      return (!field.equals(param) && !"VALUE".equalsIgnoreCase(param));
    }
    return false;
  }
  
  protected List extractFieldsFromLookupQuery(String qry) throws InvalidLookupQueryException, Exception {
    String originalQuery = qry;
    if (qry == null)
      return null;
    List fields = new ArrayList();
    while (qry.indexOf('#') > -1){
      int loc = qry.indexOf('#');
      int loc2 = qry.indexOf('#', loc + 1);
      if (loc < 0 || loc2 < 0 || (loc2 - loc) < 2)
        throw new InvalidLookupQueryException(originalQuery);
      fields.add(qry.substring(loc + 1, loc2));
      qry = qry.substring(loc2 + 1, qry.length());
    }
    return cleanFields(fields);
  }

  private List cleanFields(List dirtyFields){
    if (dirtyFields == null || dirtyFields.size() == 0)
      return dirtyFields;
    List cleanFields = new ArrayList();
    for (int i = 0; i < dirtyFields.size(); i++){
      String tmp = (String)dirtyFields.get(i);
      if (tmp != null && tmp.indexOf(':') > 0)
        tmp = tmp.substring(0, tmp.indexOf(':'));
      cleanFields.add(tmp);
    }
    return cleanFields;
  }

  public List getLookupQueryParams(int index){
    return (List)lookupQueryParams.get(index);
  }
  
  public boolean isOptionalLookup(int index) {
    return lookupOptional[index];
  }
  
  public boolean isExternal(int index) {
  	return isExternal[index];
  }

  public boolean isLookupWithMultipleParam(int index) {
    return lookupMultipleParam[index];
  }
  
  public String getFilterOn(int index){
    return filterOns[index];
  }

  public String getNullDefinition(int index){
    return nullDefinitions[index];
  }

  public String getLookupQueryName(int index) {
    return fields.get(index).toString() + "_LOOKUP";
  }

  public List getDefaultValues() {
    return defaultValues;
  }

  protected void loadTemplate(){
	  try {
	      Properties loadProps = Utilities.load("querytemplate.properties");
	      header = loadProps.getProperty(TEMPLATE_HEADER_PROP_NAME);
	      footer = loadProps.getProperty(TEMPLATE_FOOTER_PROP_NAME);
	    } catch (IOException e) {
	      throw new RuntimeException("Templates required for loading configuration missing", e);
	    }
  }
  
  public void parseDefinition(Properties defBuffer) throws InvalidLookupQueryException, InvalidRefreshDataQueryException, Exception { 
	  
	  if (objectUsage > MAXIMUM_USAGE) throw new Exception("Object should not call parseDefinition method for more than 1 time");  
	  objectUsage++;
	  
  	loadTemplate();

  	selectQueryWhere = getAndTrim(SELECT_QUERY_WHERE_KEY, defBuffer);
    controlFileSuffix = getAndTrim(CONTROL_FILE_SUFFIX, defBuffer, "");
    controlFilePrefix = getAndTrim(CONTROL_FILE_PREFIX, defBuffer, "");
    outputHeader = defBuffer.getProperty(OUTPUT_HEADER);
    outputFooter = defBuffer.getProperty(OUTPUT_FOOTER);
    fields = new ArrayList();
    lookupQueries = new ArrayList();
    lookupTypes = new ArrayList();
    defaultValues = new ArrayList();
    lookupQueryParams = new ArrayList();
    final List starts = new ArrayList();
    final List ends = new ArrayList();
    for (Iterator i = defBuffer.keySet().iterator(); i.hasNext();) {
      String key = (String) i.next();
      if(key.endsWith(LOCATION_SUFFIX))
        fields.add(key.substring(0, key.length() - LOCATION_SUFFIX.length()));
    }
    isExternal = new boolean[fields.size()];
    lookupOptional = new boolean[fields.size()];
    lookupMultipleParam = new boolean[fields.size()];
    handlerClasses = new Class[fields.size()];
    handlerArgs = new String[fields.size()];
    filterOns = new String[fields.size()];
    nullDefinitions = new String[fields.size()];

    outputType = defBuffer.getProperty(OUT_TYPE_KEY, RecordDispenserFactory.FIXED_WIDTH).trim();
    if (isDelimitedFormat()){
    	delimiter = defBuffer.getProperty(DELIMITER);
    	if (delimiter == null || delimiter.length() == 0)
    		throw new RuntimeException("Missing delimiter information for delimited output file");
    } else if (isXMLFormat()){
    	xmlRootTag = defBuffer.getProperty(XML_ROOT_TAG);
    	if (xmlRootTag == null || xmlRootTag.length() == 0)
    		throw new RuntimeException("Missing root tag for xml output file");
    	xmlStartTag = defBuffer.getProperty(XML_START_TAG);
    	if (xmlStartTag == null || xmlStartTag.length() == 0)
    		throw new RuntimeException("Missing record start tag for xml output file");
    }

    int index = 0;
    for (Iterator i = fields.iterator(); i.hasNext(); index++) {
      String field = (String) i.next();
      String location = (String) defBuffer.get(field + LOCATION_SUFFIX);
      getLocation(field, location, starts, ends);
      String external = (String) defBuffer.get(field + IS_EXTERNAL_SUFFIX);
      isExternal[index] = external != null && external.toUpperCase().startsWith("T");
      String optional = (String) defBuffer.get(field + IS_OPTIONAL_SUFFIX);
      lookupOptional[index] = optional != null && optional.toUpperCase().startsWith("T");
      String lookupQuery = getAndTrim(field + LOOKUP_QUERY_SUFFIX, defBuffer);
      String lookupType = getAndTrim(field + LOOKUP_DATA_TYPE, defBuffer);
      List lookupQueryParam = extractFieldsFromLookupQuery(lookupQuery);
      lookupMultipleParam[index] = (lookupQueryParam != null && lookupQueryParam.size() > 1);
      String defaultValue = (String) defBuffer.get(field + DEFAULT_SUFFIX);
      handlerArgs[index] = getAndTrim(field + HANDLER_ARGS_SUFFIX, defBuffer);
      handlerClasses[index] = getHandlerClass(getAndTrim(field + HANDLER_SUFFIX, defBuffer));
      filterOns[index] = getAndTrim(field + FILTER_ON_SUFFIX, defBuffer);
      nullDefinitions[index] = getAndTrim(field + NULL_DEFINITION_SUFFIX, defBuffer);
      lookupQueries.add(lookupQuery);
      lookupTypes.add(getLookupDataType(lookupType));
      defaultValues.add(defaultValue);
      lookupQueryParams.add(lookupQueryParam);
    }
    startIndexes = new String[starts.size()];
    endIndexes = new String[ends.size()];
    Iterator s = starts.iterator();
    Iterator e = ends.iterator();
    for (int i = 0; e.hasNext(); i++) {
      String end = (String) e.next();
      String start = (String) s.next();
      startIndexes[i] = (start != null ? start.trim() : null);
      endIndexes[i] = (end != null ? end.trim() : null);
      if (isFixedWidthFormat())
      	recordSize = (recordSize < new Integer(endIndexes[i]).intValue() ? new Integer(endIndexes[i]).intValue() : recordSize);
    }
  }

  public boolean isXMLFormat() {
    return getOutputType().equalsIgnoreCase(RecordDispenserFactory.XML);
  }

  public boolean isDelimitedFormat() {
    return getOutputType().equalsIgnoreCase(RecordDispenserFactory.DELIMITED) ||
        getOutputType().equalsIgnoreCase(RecordDispenserFactory.DELIMITED_KEY_VALUE);
  }

  protected String getLookupDataType(String lookupType){
  	if (lookupType != null && lookupType.trim().equalsIgnoreCase("long"))
  		return Long.class.getName();
  	if (lookupType != null && lookupType.trim().equalsIgnoreCase("int"))
  		return Integer.class.getName();
  	if (lookupType != null && lookupType.trim().equalsIgnoreCase("date"))
  		return Date.class.getName();
  	return String.class.getName();
  }

  private void getLocation(String field, String location, List starts, List ends){
  	if (isFixedWidthFormat()){
  		String[] cut = location.split("[-]|\\s+");
    	if (cut.length < 2){
    		throw new RuntimeException(field + LOCATION_SUFFIX + " value " + location + " invalid, must be in the form <start>-<end>.");
    	}
    	starts.add(cut[0]);
    	ends.add(cut[1]);
  	} else if (isDelimitedFormat()){
  		try{
  			starts.add(location);
  			ends.add(location);
  		} catch (Exception e){
  			throw new RuntimeException(field + LOCATION_SUFFIX + " value " + location + " invalid, exception occured = " + e.getMessage());
  		}
  	} else if (isXMLFormat()){
  		if (location == null || location.trim().length() == 0){
  			starts.add(XML_NONE_LOCATION);
  			ends.add(XML_NONE_LOCATION);
  		} else {
  			starts.add(location);
  			ends.add(location);
  		}
  	}
  }

  protected Class getHandlerClass(String name) {
    if (name == null || "".equals(name))
      return PassThroughHandler.class;
    try {
      return Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Configured handler " + name + " could not be loaded");
    }    
  }

  private String getAndTrim(String key, Properties defBuffer, String maskNull) {
    String v = getAndTrim(key, defBuffer);
    if (v == null) return maskNull;
    return v;
  }
  private String getAndTrim(String key, Properties defBuffer) {
    String value = defBuffer.getProperty(key);
    if (value != null){
      value = value.trim();
    }
    return value;
  }

  public IFieldTextHandler[] buildFieldHandlers() {
    IFieldTextHandler[] handlers = new IFieldTextHandler[handlerClasses.length];
    for (int i = 0; i < handlerClasses.length; i++) {
      try {
        handlers[i] = (IFieldTextHandler) Reflect.invokeConstructor(handlerClasses[i], null, null);
        handlers[i].configure(handlerArgs[i]);
      } catch (ReflectionException e) {
        throw new RuntimeException("Unable to instantiate handler of type " + handlerClasses[i].getName());
      }
    }
    return handlers;
  }
  public List getFieldsSortedByStartIndexes(){
  	return getFieldsSortedByStartIndexes(ExtractDefinitionParser.ASCENDING);
  }
  
  public List getFieldsSortedByStartIndexes(final int order){
  	if (order != ASCENDING && order != DESCENDING)
  		throw new RuntimeException("Invalid order direction for field sorting: " + order);
  	List sortedFields = getFields();
  	Collections.sort(sortedFields, new Comparator() {
  		public int compare(Object o1, Object o2){
  			String field1StartPos = getStartIndex(getFieldIndex((String)o1));
  			String field2StartPos = getStartIndex(getFieldIndex((String)o2));
  			if (isXMLFormat())
  				return field1StartPos.compareToIgnoreCase(field2StartPos);
  			else if (isFixedWidthFormat() || isDelimitedFormat())
  				return (Integer.parseInt(field1StartPos) - Integer.parseInt(field2StartPos) * order);
  			else
  				throw new RuntimeException("Invalid output type, sorting of fields not supported.");
  		}
  	});
  	return sortedFields;
  }
  
  public int getFieldIndex(String field){
  	return fields.indexOf(field);
  }

  public String getDefaultValue(int fieldIndex){
  	return (defaultValues.get(fieldIndex) != null ? (String)defaultValues.get(fieldIndex) : null);
  }

  public boolean isControlFileRequired() {
    return !(getControlFilePrefix().equals("") && getControlFileSuffix().equals(""));
  }

  public String getControlFileSuffix() {
    return controlFileSuffix;
  }

  public String getControlFilePrefix() {
    return controlFilePrefix;
  }
  
  public String getOutputHeader(Object[] args){
  	if(outputHeader==null) return null;
  	try{
  		return new MessageFormat(outputHeader).format(args);
  	}catch(Exception e){
  		log.error("Failed to generate header. "+e.getMessage());
  	}
  	return outputHeader;
  }
  
  public String getOutputFooter(Object[] args){
  	if(outputHeader==null) return null;
  	try{
  		return new MessageFormat(outputFooter).format(args);
  	}catch(Exception e){
  		log.error("Failed to generate footerr. "+e.getMessage());
  	}
  	return outputFooter;
  }
}