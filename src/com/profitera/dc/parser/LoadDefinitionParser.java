/*
 * Created on Feb 25, 2005
 *
 */
package com.profitera.dc.parser;

import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.profitera.dc.InvalidLookupDefinitionException;
import com.profitera.dc.InvalidLookupQueryException;
import com.profitera.dc.InvalidRefreshDataQueryException;
import com.profitera.dc.RecordDispenserFactory;
import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.handler.LongHandler;
import com.profitera.dc.handler.PassThroughHandler;
import com.profitera.dc.parser.exception.InvalidParserConfiguration;
import com.profitera.ibatis.SQLMapFileRenderer;
import com.profitera.util.Filter;
import com.profitera.util.MapCar;
import com.profitera.util.Strings;
import com.profitera.util.Utilities;
import com.profitera.util.reflect.Reflect;
import com.profitera.util.reflect.ReflectionException;

/**
 * @deprecated use V2LoadDefinitionParser
 * @author jamison
 */
public class LoadDefinitionParser {
  
  public static final String MIXED_MODE = "mixed";
  public static final String UPDATE_MODE = "update";
  public static final String INSERT_MODE = "insert";


	private static final String LOOKUP_VALUE_RESULT = "LOOKUP_VALUE_RESULT";

  public static final String[] RESERVED_DELIMITERS = {"|"}; 
			
	public static final String SPACE = "SPACE";
	public static final String TAB = "TAB";
	
	public static final String XML_NONE_LOCATION = "NONE";
	public static final String XML_START_TAG = "recordIndicatorTag";
	public static final String XQUERY = "xQuery";
	
	private static final String TEMPLATE_FOOTER_PROP_NAME = "footer";
	private static final String TEMPLATE_HEADER_PROP_NAME = "header";
	private static final String TEMPLATE_KEY_KEY_START_PROP_NAME = "selectKeyStart";
	private static final String TEMPLATE_KEY_KEY_END_PROP_NAME = "selectKeyEnd";
	private static final String TEMPLATE_IS_PREFIX_SELECTKEY_NAME = "selectKeyIsPrefix";

	protected static final Log log = LogFactory.getLog(LoadDefinitionParser.class);

	/**
   * @author jamison
   */
  public class RemoveKeysFilter extends Filter {

    /* (non-Javadoc)
     * @see com.profitera.util.Filter#filter(java.lang.Object)
     */
    protected boolean filter(Object o) {
      return keyFields.indexOf(o) < 0;
    }

  }

  private final class RemoveExternalFilter extends Filter {
    private final List fields;

    private RemoveExternalFilter(List fields) {
      super();
      this.fields = fields;
    }

    protected boolean filter(Object o) {
      int i = fields.indexOf(o);
      if (i >= 0)
        return !isExternal[i];
      return true;
    }
  }

  protected static final String LOAD_TYPE_KEY = "loadType";
  protected static final String DELIMITER = "delimiter";
  protected static final String SELECT_QUERY_WHERE_KEY = "selectQueryWhere";
  protected static final String UPDATE_QUERY_WHERE_KEY = "updateQueryWhere";
  protected static final String POST_INSERT_UPDATE_KEY = "postInsertUpdate";
  protected static final String POST_INSERT_INSERTION_KEY = "postInsertInsertion";
  protected static final String POST_UPDATE_UPDATE_KEY = "postUpdateUpdate";
  protected static final String POST_UPDATE_INSERTION_KEY = "postUpdateInsertion";
  protected static final String IS_OPTIONAL_SUFFIX = "_isOptional";
  protected static final String IS_EXTERNAL_SUFFIX = "_isExternal";
  protected static final String DEFAULT_SUFFIX = "_default";
  protected static final String LOOKUP_QUERY_SUFFIX = "_lookupQuery";
  protected static final String LOOKUP_DATA_TYPE = "_lookupType";
  protected static final String LOOKUP_INSERT_SUFFIX = "_lookupInsert";
  protected static final String LOOKUP_KEY_GEN_SUFFIX = "_lookupInsertKey";
  protected static final String IS_KEY_SUFFIX = "_isKey";
  protected static final String LOCATION_SUFFIX = "_location";
  protected static final String HANDLER_SUFFIX = "_handler";
  protected static final String HANDLER_ARGS_SUFFIX = "_handlerConfig";
  protected static final String FILTER_ON_SUFFIX = "_filterOn";
  protected static final String NULL_DEFINITION_SUFFIX = "_nullDefinition";
  protected static final String CACHE = "_cache";
  protected static final String GENERATED_KEY_KEY = "generatedKey";
  protected static final String GENERATED_KEY_SEQ_NAME_KEY = "generatedKeySeqName";
  protected static final String TABLE_KEY = "table";
  protected static final String PAD_LINE_KEY = "padLine";
  protected static final String REFRESH_DATA_KEY = "refreshData";
  protected static final String REFRESH_DATA_QUERY = "refreshDataQuery";
  protected static final String REFRESH_DATA_QUERY_TIMEOUT = "refreshDataQueryTimeout"; //defaults to -1, which means follow whatever specified globally
  protected static final String HEADER_NAME_KEY = "headerName";
  protected static final String TRAILER_NAME_KEY = "trailerName";
  protected static final Class MAP_IMPL_CLASS = HashMap.class;
  protected static final String MAP_IMPL = MAP_IMPL_CLASS.getName();
  
  public static final int REFRESH_DATA_UPDATE = 1;
  public static final int REFRESH_DATA_DELETE = 2;

  public final static int ASCENDING = 1;
  public final static int DESCENDING = -1;
  
  protected boolean isFullCacheEnabled = false;  
  protected String header;
  protected String footer;
  protected String loadType = RecordDispenserFactory.FIXED_WIDTH;
  protected String delimiter;
  protected String xmlStartTag;
  protected String xQuery;
  protected List fields;
  protected List keyFields;
  protected List lookupQueries;
  protected List lookupTypes;
  protected List lookupInserts;
  protected List lookupSelectKeys;
  protected List lookupFullCacheQueries;
  protected boolean[] lookupCached;
  protected List defaultValues;
  protected List lookupQueryParams;
  protected String[] startIndexes;
  protected String[] endIndexes;
  protected String destTable;
  protected String generatedKeyField;
  protected String generatedKeySeqName;
  protected String postInsertInsertion;
  protected String postInsertUpdate;
  protected String postUpdateInsertion;
  protected String postUpdateUpdate;
  protected StringBuffer sourceText;
  protected String selectQueryWhere;
  protected String updateQueryWhere;
  protected boolean[] isExternal;
  protected boolean[] lookupOptional;
  protected boolean[] lookupMultipleParam;
  protected Class[] handlerClasses;
  protected String[] handlerArgs;
  protected String[] filterOns;
  protected String[] filterOnFields;
  protected String[] nullDefinitions;
  protected String selectKeyStart;
  protected String selectKeyEnd;
  protected boolean padLine = true;
  protected boolean refreshData = false;
  protected String refreshDataQuery;
  protected int refreshDataQueryType = REFRESH_DATA_DELETE;
  protected long refreshDataQueryTimeout = -1;
  protected List headerFieldText = new ArrayList();
  protected List trailerFieldText = new ArrayList();
  protected boolean isPrefixSelectKey = true;
  
  protected int recordSize = 0;
  protected int objectUsage = 0;
  protected static final int MAXIMUM_USAGE = 1;

  private static final String SELECT_ALL_CACHE_COUNT_PROP = "COUNT_ROW_EXISTS";

  private static final String SELECT_ALL_CACHE_STATEMENT_NAME = "FULL_CACHE_QUERY";
  
  public LoadDefinitionParser(){
	  objectUsage++;
  }
  
  public String getLoadType(){
  	return loadType;
  }
  
  public boolean isFullCacheEnabled(){
    return isFullCacheEnabled;
  }
  
  public String getDelimiter(){
  	if(delimiter==null) return null;
  	if (delimiter.equalsIgnoreCase(SPACE))
  		return " ";
  	if (delimiter.equalsIgnoreCase(TAB))
  		return "\t";
  	for (int i = 0; i < RESERVED_DELIMITERS.length; i++){
  		if (delimiter.equals(RESERVED_DELIMITERS[i]))
  			delimiter = "\\" + delimiter;
  		break;
  	}
  	return delimiter;
  }
  
  public String getXMLStartTag(){
  	return xmlStartTag;
  }

  public String getXQuery(){
  	return xQuery;
  }
  
  public String getDestinationTable(){
    return destTable;
  }
  
  public boolean padLine() {
    return padLine;
  }
  
  public boolean refreshData(){
    return refreshData;
  }
  
  public int getRefreshDataQueryType(){
  	return refreshDataQueryType;
  }
  
  public List getHeaderFieldText() {
    return headerFieldText;
  }

  public List getTrailerFieldText() {
    return trailerFieldText;
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
  
  public int getRecordSize() {
    return recordSize;
  }
  /**
   * @param xmlBuffer
   * @throws IOException
   */
  public void writeQueriesToBuffer(final BufferedWriter xmlBuffer) throws IOException {
  	xmlBuffer.write(header + "\n");
    xmlBuffer.write("<!--\n");
    xmlBuffer.write(sourceText + "\n");
    xmlBuffer.write(keyFields.toString() + "\n");
    xmlBuffer.write(fields.toString() + "\n");
    xmlBuffer.write(defaultValues.toString() + "\n");
    xmlBuffer.write(lookupQueries.toString() + "\n");
    xmlBuffer.write("-->\n");
    writeQueries(xmlBuffer, destTable, fields, keyFields);
    xmlBuffer.write(footer);
  }

  /**
   * @param xmlBuffer
   * @param fields
   * @param keyFields
   * @throws IOException
   */
  private void writeQueries(final BufferedWriter xmlBuffer, final String tableName, final List fields, final List keyFields) throws IOException {
    writeResultMap(xmlBuffer, tableName, fields);
    writeSelectQuery(xmlBuffer, tableName, keyFields, fields);
    writeSelectCountQuery(xmlBuffer, tableName, keyFields);
    writeSelectAllCacheQuery(xmlBuffer, tableName, keyFields);
    writeUpdateQuery(xmlBuffer, tableName, keyFields, fields);
    writeInsertQuery(xmlBuffer, tableName, fields);
    if (refreshData())
      writeRefreshQuery(xmlBuffer, tableName);
    if (hasPostInsertUpdate()){
      writePostInsertUpdate(xmlBuffer);
    }
    if (hasPostInsertInsertion()){
      writePostInsertInsertion(xmlBuffer);
    }
    if (hasPostUpdateUpdate()){
      writePostUpdateUpdate(xmlBuffer);
    }
    if (hasPostUpdateInsert()){
      writePostUpdateInsertion(xmlBuffer);
    }
    writeLookupQueries(xmlBuffer);
  }

  /**
   * @param xmlBuffer
   * @throws IOException
   */
  private void writePostInsertInsertion(BufferedWriter xmlBuffer) throws IOException {
    xmlBuffer.write("<insert id=" + Strings.quoteString(getPostInsertInsertionName()));
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n");
    xmlBuffer.write("<![CDATA[\n");
    xmlBuffer.write(postInsertInsertion);
    xmlBuffer.write("\n ]]>\n</insert>\n");
  }

  private void writePostUpdateInsertion(BufferedWriter xmlBuffer) throws IOException {
    xmlBuffer.write("<insert id=" + Strings.quoteString(getPostUpdateInsertName()));
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n");
    xmlBuffer.write("<![CDATA[\n");
    xmlBuffer.write(postUpdateInsertion);
    xmlBuffer.write("\n ]]>\n</insert>\n");
  }

  /**
   * @return
   */
  public String getPostInsertInsertionName() {
    return destTable + "_POST_INSERT_INSERTION";
  }

  /**
   * @return
   */
  public boolean hasPostInsertInsertion() {
    return postInsertInsertion != null;
  }

  /**
   * @param xmlBuffer
   * @throws IOException
   */
  private void writePostInsertUpdate(BufferedWriter xmlBuffer) throws IOException {
    xmlBuffer.write("<update id=" + Strings.quoteString(getPostInsertUpdateName()));
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n  <![CDATA[\n");
    xmlBuffer.write(postInsertUpdate);
    xmlBuffer.write("\n  ]]>\n</update>\n");
  }

  private void writePostUpdateUpdate(BufferedWriter xmlBuffer) throws IOException {
    xmlBuffer.write("<update id=" + Strings.quoteString(getPostUpdateUpdateName()));
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n  <![CDATA[\n");
    xmlBuffer.write(postUpdateUpdate);
    xmlBuffer.write("\n  ]]>\n</update>\n");
  }

  /**
   * @param xmlBuffer
   */
  private void writeLookupQueries(BufferedWriter xmlBuffer) throws IOException {
    for (int i = 0; i < lookupQueries.size(); i++) {
      if (lookupQueries.get(i) != null){
        writeLookupQuery(getLookupQueryName(i), (String) lookupQueries.get(i), (String)lookupTypes.get(i), xmlBuffer);
      }
      if (lookupInserts.get(i) != null){
        writeLookupInsert(getLookupInsertName(i), (String) lookupSelectKeys.get(i), (String) lookupInserts.get(i), (String)lookupTypes.get(i), xmlBuffer);
      }
      if (lookupFullCacheQueries.get(i) != null){
        writeLookupFullCacheQuery(i, getLookupFullCacheQueryName(i), (String) lookupFullCacheQueries.get(i), (String)lookupTypes.get(i), getLookupQueryParams(i), xmlBuffer);
      }
    }
  }

  private void writeRefreshQuery(BufferedWriter xmlBuffer, String tableName) throws IOException {
  	String refreshSql = null;
  	if (refreshDataQuery == null){
  		refreshSql = "<delete id=" + Strings.quoteString(getRefreshQueryName());
  		if (refreshDataQueryTimeout >= 0)
  			refreshSql += " timeout=" + Strings.quoteString("" + refreshDataQueryTimeout);
  		refreshSql += ">\n";
  		refreshSql +=  "\tDELETE FROM " + tableName.toUpperCase() + "\n";
  		refreshSql += "</delete>";
  	}
  	else if (getRefreshDataQueryType() == REFRESH_DATA_DELETE){
  		refreshSql = "<delete id=" + Strings.quoteString(getRefreshQueryName());
  		if (refreshDataQueryTimeout >= 0)
  			refreshSql += " timeout=" + Strings.quoteString("" + refreshDataQueryTimeout);
  		refreshSql += ">\n";
  		refreshSql += "\t<![CDATA[" + refreshDataQuery + "]]>\n";
  		refreshSql += "</delete>";
  	}
  	else if (getRefreshDataQueryType() == REFRESH_DATA_UPDATE){
  		refreshSql = "<update id=" + Strings.quoteString(getRefreshQueryName());
  		if (refreshDataQueryTimeout >= 0)
  			refreshSql += " timeout=" + Strings.quoteString("" + refreshDataQueryTimeout);
  		refreshSql += ">\n";
  		refreshSql += "\t<![CDATA[" + refreshDataQuery + "]]>\n";
  		refreshSql += "</update>";
  	}
  	xmlBuffer.write(refreshSql);
  }

  
  /**
   * @param lookupInsertName
   * @param string
   * @param string2
   * @param xmlBuffer
   * @throws IOException
   */
  private void writeLookupInsert(String lookupInsertName, String selectKey, String lookupInsert, String dataType, BufferedWriter xmlBuffer) throws IOException {
    xmlBuffer.write("<insert id=" + Strings.quoteString(lookupInsertName));
    //jambu changed this from Map.getClass().getName() to MAP_IMPL
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n");
    if (selectKey != null && isPrefixSelectKey){
    	writeSelectKey("ID", selectKey, dataType, xmlBuffer);
    }
    xmlBuffer.write("<![CDATA[\n");
    xmlBuffer.write(lookupInsert + "\n");
    xmlBuffer.write("\n  ]]>\n");
    if (selectKey != null && !isPrefixSelectKey){
      writeSelectKey("ID", selectKey, dataType, xmlBuffer);
    }
    xmlBuffer.write("</insert>\n");
  }

  /**
   * @param selectKey
   * @param xmlBuffer
   * @throws IOException
   */
  private void writeSelectKey(BufferedWriter xmlBuffer) throws IOException {
    String sequenceName = destTable + "_" + generatedKeyField;
    if (generatedKeySeqName != null && generatedKeySeqName.length() != 0)
      sequenceName = generatedKeySeqName;
    String q = selectKeyStart + sequenceName + selectKeyEnd;
    writeSelectKey(generatedKeyField, q, "long", xmlBuffer);
    
  }

  private void writeLookupQuery(String name, String sqltext, String dataType, BufferedWriter xmlBuffer) throws IOException{
    xmlBuffer.write("<select id=" + Strings.quoteString(name));
    xmlBuffer.write(" resultClass=" + Strings.quoteString(dataType));
    //jambu changed this from String.getClass().getName() to MAP_IMPL
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n  <![CDATA[\n");
    xmlBuffer.write(sqltext);
    xmlBuffer.write("\n  ]]>\n</select>\n");
  }
  
  private void writeLookupFullCacheQuery(int fieldIndex, String queryName, String sqltext, String dataType, List lookupParameters, BufferedWriter xmlBuffer) throws IOException{
    SQLMapFileRenderer r = new SQLMapFileRenderer();
    if (lookupParameters == null) lookupParameters = java.util.Collections.EMPTY_LIST;
    String[] props = new String[lookupParameters.size() + 1];
    Class[] classes = new Class[props.length];
    props[0] = LOOKUP_VALUE_RESULT;
    try {
      classes[0] = Class.forName(dataType);
    } catch (ClassNotFoundException e) { 
      // Unreachable
    }
    for(int i = 1; i < props.length; i++){
      props[i] = (String) lookupParameters.get(i-1);
      int paramIndex = getFieldIndex(props[i]);
      // If the lookup param is the field itself it has to map as a string.
      // B/c it it coming from the file to be looked up by this query, duh!
      if (paramIndex != fieldIndex && getLookupQueryName(getFieldIndex(props[i])) != null){
        Class c = getLookupClassForField(paramIndex);
        classes[i] = c;
      } else {
        classes[i] = String.class;
      }
    }
    xmlBuffer.write(r.renderResultMap(queryName + "-rmap", MAP_IMPL_CLASS, props, classes));
    xmlBuffer.write(r.renderSelect(queryName, queryName + "-rmap", sqltext));
  }

  public Class getLookupClassForField(int paramIndex) {
    Class c = null;
    // Not a lookup, return null
    if (lookupQueries.get(paramIndex) == null) return null;
    String t = (String) lookupTypes.get(paramIndex);
    try {
      c = Class.forName(t);
    } catch (ClassNotFoundException e) { 
      // Unreachable
    }
    return c;
  }

  /**
   * @param xmlBuffer
   * @param tableName
   * @param fields
   * @throws IOException
   */
  private void writeInsertQuery(BufferedWriter xmlBuffer, String tableName, List fields) throws IOException {
    xmlBuffer.write("<insert id=" + Strings.quoteString(getInsertName()));
    xmlBuffer.write(" parameterMap=" + Strings.quoteString(getInsertParamMapName(tableName)));
    xmlBuffer.write(">\n");
    if (generatedKeyField != null && isPrefixSelectKey){
      writeSelectKey(xmlBuffer);
    }
    List fieldsPlusGenKey = getFieldsPlusGenKey(fields);
    xmlBuffer.write("<![CDATA[\n");
    xmlBuffer.write("    insert into " + tableName + "\n");
    xmlBuffer.write("    (" + Strings.getListString(fieldsPlusGenKey, ", ") + ")");
    xmlBuffer.write("    values (" + Strings.getListString(MapCar.map(new MapCar(){
      public Object map(Object o) {
        return "?";
      }}, fieldsPlusGenKey), ", "));
    xmlBuffer.write(")");
    xmlBuffer.write("\n  ]]>\n");
    if (generatedKeyField != null && !isPrefixSelectKey){
      writeSelectKey(xmlBuffer);
    }
    xmlBuffer.write("</insert>\n");
  }

  /**
   * @param fields
   * @return
   */
  private List getFieldsPlusGenKey(List fields) {
    List fieldsPlusGenKey = new ArrayList(fields);
    if (generatedKeyField != null){
      fieldsPlusGenKey.add(generatedKeyField);
    }
    fieldsPlusGenKey = new RemoveExternalFilter(fields).filterItems(fieldsPlusGenKey);
    return fieldsPlusGenKey;
  }

  /**
   * @param tableName
   * @return
   */
  private String getInsertParamMapName(String tableName) {
    return "insert-" + tableName;
  }

  /**
   * @param xmlBuffer
   * @throws IOException
   */
  private void writeSelectKey(String generatedKeyField, String query, String dataType, BufferedWriter xmlBuffer) throws IOException {
    xmlBuffer.write("<selectKey resultClass=" + Strings.quoteString(dataType) + " keyProperty=" + Strings.quoteString(generatedKeyField) + ">\n");
    xmlBuffer.write("      " + query + "\n");
    xmlBuffer.write("</selectKey>\n");
  }

  /**
   * @param tableName
   * @return
   */
  public String getInsertName() {
    return destTable + "_INSERT";
  }

  /**
   * @param xmlBuffer
   * @param tableName
   * @param keyFields
   * @param fields
   * @throws IOException
   */
  private void writeUpdateQuery(BufferedWriter xmlBuffer, String tableName, List keyFields, List fields) throws IOException {
    xmlBuffer.write("<update id=" + Strings.quoteString(getUpdateName()));
    xmlBuffer.write(" parameterMap=" + Strings.quoteString(getParamMapName(tableName)));
    xmlBuffer.write(">\n  <![CDATA[\n");
    xmlBuffer.write("    update " + tableName + " set " + Strings.getListString(MapCar.map(new MapCar(){
      public Object map(Object o) {
        return o.toString() + "= ?";
      }}, new RemoveKeysFilter().filterItems(new RemoveExternalFilter(fields).filterItems(fields))), ",\n      "));
    xmlBuffer.write(getUpdateWhereClause(keyFields, true));
    xmlBuffer.write("\n ]]>\n</update>\n");
  }
  /**
   * @param tableName
   * @return
   */
  public String getUpdateName() {
    return destTable + "_UPDATE";
  }

  /**
   * @param xmlBuffer
   * @param tableName
   * @param keyFields
   * @param fields
   * @throws IOException
   */
  private void writeSelectCountQuery(BufferedWriter xmlBuffer, String tableName, List keyFields) throws IOException {
    xmlBuffer.write("<select id=" + Strings.quoteString(getSelectCountName()));
    xmlBuffer.write(" resultClass=" + Strings.quoteString("long"));
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n  <![CDATA[\n");
    xmlBuffer.write("    select count(*)");
    xmlBuffer.write("    from " + tableName + "\n");
    xmlBuffer.write(getWhereClause(keyFields, false));
    xmlBuffer.write("\n  ]]>\n</select>\n");
  }
  

  private void writeSelectAllCacheQuery(BufferedWriter xmlBuffer,
      String tableName, List keyFields) throws IOException {
    String sql = "select 1";
    for (Iterator i = keyFields.iterator(); i.hasNext();) {
      String f = (String) i.next();
      sql = sql + ", " + f;
    }
    sql = sql + " from " + tableName;
    SQLMapFileRenderer r = new SQLMapFileRenderer();
    String[] props = new String[keyFields.size()+1];
    Class[] javaTypes = new Class[props.length];
    props[0] = getFullCacheValueName();
    javaTypes[0] = Long.class;
    for (int i = 1; i < props.length; i++) {
      String field = (String) keyFields.get(i-1);
      props[i] = field;
      javaTypes[i] = getLookupClassForField(getFieldIndex(field));
      if (javaTypes[i] == null) javaTypes[i] = String.class;
    }
    String rmapName = getFullCacheQueryName() + "-rmap";
    xmlBuffer.write(r.renderResultMap(rmapName, MAP_IMPL_CLASS, props, javaTypes));
    xmlBuffer.write(r.renderSelect(getFullCacheQueryName(), rmapName, sql));
  }
  
  public String getFullCacheQueryName(){
    return destTable + "_" + SELECT_ALL_CACHE_STATEMENT_NAME;
  }
  public String getFullCacheValueName(){
    return SELECT_ALL_CACHE_COUNT_PROP;
  }
  
  /**
   * @param tableName
   * @return
   */
  public String getSelectCountName() {
    return getSelectName() + "_COUNT";
  }

  private void writeSelectQuery(BufferedWriter xmlBuffer, String tableName, List keyFields, final List fields) throws IOException {
    xmlBuffer.write("<select id=" + Strings.quoteString(getSelectName()));
    xmlBuffer.write(" resultMap=" + Strings.quoteString(getSelectResultMapName(tableName)));
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n  <![CDATA[\n");
    xmlBuffer.write("    select " + Strings.getListString(new RemoveExternalFilter(fields).filterItems(fields), ", "));
    xmlBuffer.write("    from " + tableName + "\n");
    xmlBuffer.write(getWhereClause(keyFields, false));
    xmlBuffer.write("\n  ]]>\n</select>\n");
  }

  /**
   * @param keyFields
   * @return
   */
  private String getWhereClause(List keyFields, final boolean useQs) {
    if (selectQueryWhere != null){
      return "   " + selectQueryWhere;
    }
    return "    where " + Strings.getListString(MapCar.map(new MapCar(){
      public Object map(Object o) {
        if (useQs)
          return o.toString() + "= ?"; // " = #" + o.toString() + "#";
        else
          return o.toString() + " = #" + o.toString() + "#";
      }}, keyFields), "\n      and ");
  }
  
  private String getUpdateWhereClause(List keyFields, final boolean useQs) {
    if (updateQueryWhere != null){
      return "   " + updateQueryWhere;
    }
    return "    where " + Strings.getListString(MapCar.map(new MapCar(){
      public Object map(Object o) {
        if (useQs)
          return o.toString() + "= ?"; // " = #" + o.toString() + "#";
        else
          return o.toString() + " = #" + o.toString() + "#";
      }}, keyFields), "\n      and ");
  }
  
  

  /**
   * @param tableName
   * @return
   */
  public String getSelectName() {
    return destTable + "_SELECT";
  }

  /**
   * @param xmlBuffer
   * @param tableName
   * @param fields
   * @throws IOException
   */
  private void writeResultMap(final BufferedWriter xmlBuffer, final String tableName, final List fields) throws IOException {
    //<resultMap id="PTRCUSTOMER_SELECT_MAP" class="java.util.HashMap">
    //  <result property="accountId" column="ACCOUNT_ID" javaType="double"/>
    //  <result property="cycDelId" column="CYC_DEL_ID" javaType="double"/>
    //</resultMap>
    xmlBuffer.write("<resultMap id=" + Strings.quoteString(getSelectResultMapName(tableName)));
    xmlBuffer.write(" class=" + Strings.quoteString(MAP_IMPL) + ">\n");
    List updateFields = new RemoveExternalFilter(fields).filterItems(fields);
    for (Iterator i = updateFields.iterator(); i.hasNext();) {
      String field = (String) i.next();
      xmlBuffer.write("<result property=" + Strings.quoteString(field) + " column=" + Strings.quoteString(field) + "/>\n"); //  javaType="double"
    }
    xmlBuffer.write("</resultMap>\n");
    //
    xmlBuffer.write("<parameterMap id=" + Strings.quoteString(getParamMapName(tableName)));
    xmlBuffer.write(" class=" + Strings.quoteString(Map.class.getName()) + ">\n");
    IFieldTextHandler[] handlers = buildFieldHandlers();
    for (Iterator i = updateFields.iterator(); i.hasNext();) {
      String field = (String) i.next();
      int index = fields.indexOf(field);
      Class valueType = handlers[index].getValueType();
      String jdbcType = getJDBCType(valueType);
      if (keyFields.indexOf(field) >= 0)
        continue;
      xmlBuffer.write("<parameter property=" + Strings.quoteString(field) 
          + " javaType=" + Strings.quoteString(valueType.getName()) 
          + " jdbcType=" + Strings.quoteString(jdbcType)
          + "/>\n"); //  javaType="double"
    }
    for (Iterator i = keyFields.iterator(); i.hasNext();) {
      String field = (String) i.next();
      int index = fields.indexOf(field);
      Class valueType = handlers[index].getValueType();
      String jdbcType = getJDBCType(valueType);
      xmlBuffer.write("<parameter property=" + Strings.quoteString(field) + " javaType=" + Strings.quoteString(valueType.getName()) + " jdbcType=" + Strings.quoteString(jdbcType) + "/>\n"); //  javaType="double"
    }
    xmlBuffer.write("</parameterMap>\n");
    //
    xmlBuffer.write("<parameterMap id=" + Strings.quoteString(getInsertParamMapName(tableName)));
    xmlBuffer.write(" class=" + Strings.quoteString(Map.class.getName()) + ">\n");
    List insertFields = updateFields;
    for (Iterator i = insertFields.iterator(); i.hasNext();) {
      String field = (String) i.next();
      int index = fields.indexOf(field);
      Class valueType = handlers[index].getValueType();
      String jdbcType = getJDBCType(valueType);
      xmlBuffer.write("<parameter property=" + Strings.quoteString(field) 
          + " javaType=" + Strings.quoteString(valueType.getName()) 
          + " jdbcType=" + Strings.quoteString(jdbcType)
          + "/>\n"); //  javaType="double"
    }
    if (getGeneratedKeyField() != null && (getFieldIndex(getGeneratedKeyField()) < 0 || !isExternal(getFieldIndex(getGeneratedKeyField())))){
      String jdbcType = getJDBCType(Long.class);
      xmlBuffer.write("<parameter property=" + Strings.quoteString(getGeneratedKeyField()) 
        + " javaType=" + Strings.quoteString(Long.class.getName()) 
        + " jdbcType=" + Strings.quoteString(jdbcType)
        + "/>\n"); //  
    }
    xmlBuffer.write("</parameterMap>\n");
    
  }

  private String getJDBCType(Class valueType) {
    String jdbcType = "VARCHAR";
    if (valueType.getSuperclass().equals(Number.class))
      jdbcType = "NUMERIC";
    else if (valueType.equals(Date.class))
      jdbcType = "TIMESTAMP";
    else if (valueType.equals(Time.class))
      jdbcType = "TIME";
    return jdbcType;
  }

  /**
   * @param tableName
   * @return
   */
  private String getParamMapName(String tableName) {
    return "rmapname-" + tableName;
  }

  /**
   * @param tableName
   * @return
   */
  private String getSelectResultMapName(final String tableName) {
    return tableName + "_SELECT_MAP";
  }

  /**
   * @param index
   * @return
   */
  public boolean hasLookupQuery(int index) {
    return lookupQueries.get(index) != null;
  }

  public boolean hasLookupFullCache(int index) {
    return lookupFullCacheQueries.get(index) != null;
  }
  
  public boolean isReferencialLookupQuery(int index){
  	List params = getLookupQueryParams(index);
  	return isReferencialLookupQuery(params);
  }
  
  protected boolean isReferencialLookupQuery(List lookupQueryParams) {
  	if (lookupQueryParams == null)
  		return false;
  	for (int i = 0; i < lookupQueryParams.size(); i++) {
  		if (getFieldIndex((String)lookupQueryParams.get(i)) > -1)
  			return true;
  	}
  	return false;
  }
  
  protected List extractFieldsFromLookupQuery(String qry) throws InvalidLookupQueryException, Exception {
    String originalQuery = qry;
    if (qry == null)
      return new ArrayList();
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

  public String getFilterOnField(int index){
    return filterOnFields[index];
  }
  
  public String getNullDefinition(int index){
    return nullDefinitions[index];
  }
  
  public boolean isCachingEnabled(int index){
  	return lookupCached[index];
  }

  /**
   * @param index
   * @return
   */
  public String getLookupQueryName(int index) {
    return fields.get(index).toString() + "_LOOKUP";
  }

  /**
   * @return
   */
  public List getDefaultValues() {
    return defaultValues;
  }
  
  protected void loadTemplate(){
	  try {
	      Properties loadProps = Utilities.load("querytemplate.properties");
	      
	      header = loadProps.getProperty(TEMPLATE_HEADER_PROP_NAME);
	      footer = loadProps.getProperty(TEMPLATE_FOOTER_PROP_NAME);
	      isPrefixSelectKey = loadProps.getProperty(TEMPLATE_IS_PREFIX_SELECTKEY_NAME, "T").toUpperCase().startsWith("T");
	      selectKeyStart = loadProps.getProperty(TEMPLATE_KEY_KEY_START_PROP_NAME);
	      if (selectKeyStart == null)
	        selectKeyStart = "";
	      selectKeyEnd = loadProps.getProperty(TEMPLATE_KEY_KEY_END_PROP_NAME);
	      if (selectKeyEnd == null){
	        selectKeyEnd = "";
	      }else if(selectKeyEnd.toLowerCase().startsWith("as") || selectKeyEnd.toLowerCase().startsWith("from")){
	      	selectKeyEnd = " "+selectKeyEnd;
	      }
	    } catch (IOException e) {
	      throw new RuntimeException("Templates required for loading configuration missing", e);
	    }
  }

  public void parseDefinition(Properties defBuffer) throws InvalidLookupQueryException, InvalidRefreshDataQueryException, Exception {
	  if (objectUsage > MAXIMUM_USAGE) throw new Exception("Object should not call parseDefinition method for more than 1 time");
	  
	  objectUsage++;
	  loadTemplate();
    loadType = defBuffer.getProperty(LOAD_TYPE_KEY, RecordDispenserFactory.FIXED_WIDTH);
    if (getLoadType().equalsIgnoreCase(RecordDispenserFactory.DELIMITED)){
    	delimiter = defBuffer.getProperty(DELIMITER);
    	if (delimiter == null || delimiter.length() == 0)
    		throw new RuntimeException("Missing delimiter information for delimited load file");
    } else if (getLoadType().equalsIgnoreCase(RecordDispenserFactory.XML)){
    	xmlStartTag = defBuffer.getProperty(XML_START_TAG);
    	if (xmlStartTag == null || xmlStartTag.length() == 0)
    		throw new RuntimeException("Missing record start tag for xml load file");
    	xQuery = defBuffer.getProperty(XQUERY);
    }

    generatedKeyField = null;
    generatedKeySeqName = null;
    destTable = getAndTrim(TABLE_KEY, defBuffer);
    generatedKeyField = getAndTrim(GENERATED_KEY_KEY, defBuffer);
    generatedKeySeqName = getAndTrim(GENERATED_KEY_SEQ_NAME_KEY, defBuffer);
    selectQueryWhere = getAndTrim(SELECT_QUERY_WHERE_KEY, defBuffer);
    updateQueryWhere = getAndTrim(UPDATE_QUERY_WHERE_KEY, defBuffer);
    postInsertUpdate = getAndTrim(POST_INSERT_UPDATE_KEY, defBuffer);
    postInsertInsertion = getAndTrim(POST_INSERT_INSERTION_KEY, defBuffer);
    postUpdateUpdate = getAndTrim(POST_UPDATE_UPDATE_KEY, defBuffer);
    postUpdateInsertion = getAndTrim(POST_UPDATE_INSERTION_KEY, defBuffer);
    
    if (defBuffer.getProperty(PAD_LINE_KEY) != null)
      padLine = Boolean.valueOf(defBuffer.getProperty(PAD_LINE_KEY)).booleanValue();
    if (defBuffer.getProperty(REFRESH_DATA_KEY) != null)
      refreshData = Boolean.valueOf(defBuffer.getProperty(REFRESH_DATA_KEY)).booleanValue();
    refreshDataQuery = defBuffer.getProperty(REFRESH_DATA_QUERY);
    if (refreshData && refreshDataQuery != null && refreshDataQuery.trim().length() != 0){
    	refreshDataQuery = refreshDataQuery.trim();
    	if (refreshDataQuery.toLowerCase().startsWith("update"))
    		refreshDataQueryType = REFRESH_DATA_UPDATE;
    	else if (refreshDataQuery.toLowerCase().startsWith("delete"))
    		refreshDataQueryType = REFRESH_DATA_DELETE;
    	else
    		throw new InvalidRefreshDataQueryException(refreshDataQuery);
    }
    if (defBuffer.getProperty(REFRESH_DATA_QUERY_TIMEOUT) != null) {
    	try {
    		refreshDataQueryTimeout = Long.parseLong(defBuffer.getProperty(REFRESH_DATA_QUERY_TIMEOUT));
    	} catch (NumberFormatException nfe) {
    		throw new InvalidParserConfiguration(REFRESH_DATA_QUERY_TIMEOUT, defBuffer.getProperty(REFRESH_DATA_QUERY_TIMEOUT), nfe);
    	}
    	if (refreshDataQueryTimeout < 0)
    		throw new InvalidParserConfiguration(REFRESH_DATA_QUERY_TIMEOUT, "" + refreshDataQueryTimeout);
    }
    if (defBuffer.getProperty(HEADER_NAME_KEY) != null)
      headerFieldText = Arrays.asList(defBuffer.getProperty(HEADER_NAME_KEY).split(";"));
    if (defBuffer.getProperty(TRAILER_NAME_KEY) != null)
      trailerFieldText = Arrays.asList(defBuffer.getProperty(TRAILER_NAME_KEY).split(";"));
    
    fields = new ArrayList();
    keyFields = new ArrayList();
    lookupQueries = new ArrayList();
    lookupTypes = new ArrayList();
    lookupInserts = new ArrayList();
    lookupSelectKeys = new ArrayList();
    lookupFullCacheQueries = new ArrayList();
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
    lookupCached = new boolean[fields.size()];
    lookupMultipleParam = new boolean[fields.size()];
    handlerClasses = new Class[fields.size()];
    handlerArgs = new String[fields.size()];
    filterOns = new String[fields.size()];
    nullDefinitions = new String[fields.size()];
    
    int index = 0;
    for (Iterator i = fields.iterator(); i.hasNext(); index++) {
      String field = (String) i.next();
      String location = (String) defBuffer.get(field + LOCATION_SUFFIX);
      getLocation(field, location, starts, ends);
      String key = (String) defBuffer.get(field + IS_KEY_SUFFIX);
      boolean isKey = key != null && key.toUpperCase().startsWith("T");
      if (isKey)
        keyFields.add(field);
      String external = (String) defBuffer.get(field + IS_EXTERNAL_SUFFIX);
      isExternal[index] = external != null && external.toUpperCase().startsWith("T");
      String optional = (String) defBuffer.get(field + IS_OPTIONAL_SUFFIX);
      lookupOptional[index] = optional != null && optional.toUpperCase().startsWith("T");
      String lookupQuery = getAndTrim(field + LOOKUP_QUERY_SUFFIX, defBuffer);
      String disableCachingStr = getAndTrim(field + CACHE, defBuffer);
      boolean cache = disableCachingStr == null || disableCachingStr.trim().length() == 0 || disableCachingStr.equalsIgnoreCase("true");
      String lookupType = getAndTrim(field + LOOKUP_DATA_TYPE, defBuffer);
      List lookupQueryParam = extractFieldsFromLookupQuery(lookupQuery);
      lookupMultipleParam[index] = (lookupQueryParam != null && lookupQueryParam.size() > 1);
      String lookupInsert = getAndTrim(field + LOOKUP_INSERT_SUFFIX, defBuffer);
      String lookupSelectKey = getAndTrim(field + LOOKUP_KEY_GEN_SUFFIX, defBuffer);
      String defaultValue = (String) defBuffer.get(field + DEFAULT_SUFFIX);
      handlerArgs[index] = getAndTrim(field + HANDLER_ARGS_SUFFIX, defBuffer);
      handlerClasses[index] = getHandlerClass(getAndTrim(field + HANDLER_SUFFIX, defBuffer));
      if (handlerClasses[index].equals(PassThroughHandler.class) && lookupQuery != null){
        handlerClasses[index] = LongHandler.class;
      }
      filterOns[index] = getAndTrim(field + FILTER_ON_SUFFIX, defBuffer);
      nullDefinitions[index] = getAndTrim(field + NULL_DEFINITION_SUFFIX, defBuffer);
      lookupQueries.add(lookupQuery);
      lookupCached[index] = cache; 
      lookupTypes.add(getLookupDataType(lookupType));
      lookupInserts.add(lookupInsert);
      lookupSelectKeys.add(lookupSelectKey);
      lookupFullCacheQueries.add(null);
      defaultValues.add(defaultValue);
      if (isReferencialLookupQuery(lookupQueryParam) && !isLookupQueryParameterValid(lookupQueryParams, field, lookupQueryParam))
      	throw new InvalidLookupQueryException(lookupQuery, "Lookup query parameter references are circular.");
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
      if (getLoadType().equalsIgnoreCase(RecordDispenserFactory.FIXED_WIDTH) || getLoadType().equalsIgnoreCase(RecordDispenserFactory.DELIMITED))
      	recordSize = (recordSize < new Integer(endIndexes[i]).intValue() ? new Integer(endIndexes[i]).intValue() : recordSize);
    }
    for (int i = 0; i < fields.size(); i++) {
    	String lookupFieldName = (String) fields.get(i);
    	if (!hasLookupQuery(i))
    		continue;
    	List lookupQueryParams = getLookupQueryParams(i);
    	for (int j = 0; j < lookupQueryParams.size(); j++) {
    		String param = (String)lookupQueryParams.get(j);
    		if (fields.indexOf(param) < 0)
    			throw new InvalidLookupDefinitionException("Field " + param + " is not defined in load definition but used in the lookup query for field " + lookupFieldName);
    	}
    }
  }

  protected boolean isLookupQueryParameterValid(List allLookupQueryParams, String fieldName, List lookupQueryParam) {
  	if (allLookupQueryParams.size() == 0)
  		return true;
  	for (int i = 0; i < lookupQueryParam.size(); i++) {
  		String param = (String)lookupQueryParam.get(i);
  		if (param.equals(fieldName) || param.equalsIgnoreCase("VALUE") || param.equalsIgnoreCase("CODE"))
  			continue;
  		if (getFieldIndex(param) == -1 || !isLookupWithMultipleParam(getFieldIndex(param)))
  			continue;
  		List otherLookupQueryParams = (List)allLookupQueryParams.get(getFieldIndex(param));
			for (int k = 0; k < otherLookupQueryParams.size(); k++) {
				String otherParam = (String)otherLookupQueryParams.get(k);
	  		if (otherParam.equalsIgnoreCase("VALUE") || otherParam.equalsIgnoreCase("CODE"))
	  			continue;
				if (fieldName.equalsIgnoreCase(otherParam))
					return false;
			}
  	}
  	return true;
  }

  protected String getLookupDataType(String lookupType){
  	if (lookupType != null && lookupType.trim().equalsIgnoreCase("string"))
  		return String.class.getName();
  	if (lookupType != null && lookupType.trim().equalsIgnoreCase("date"))
  		return Date.class.getName();
  	return Long.class.getName();
  }
  
  private void getLocation(String field, String location, List starts, List ends){
  	if (getLoadType().equalsIgnoreCase(RecordDispenserFactory.FIXED_WIDTH)){
  		String[] cut = location.split("[-]|\\s+");
    	if (cut.length < 2){
    		throw new RuntimeException(field + LOCATION_SUFFIX + " value " + location + " invalid, must be in the form <start>-<end>.");
    	}
    	starts.add(cut[0]);
    	ends.add(cut[1]);
  	} else if (getLoadType().equalsIgnoreCase(RecordDispenserFactory.DELIMITED)){
  		try{
  			starts.add(location);
  			ends.add(location);
  		} catch (Exception e){
  			throw new RuntimeException(field + LOCATION_SUFFIX + " value " + location + " invalid, exception occured = " + e.getMessage());
  		}
  	} else if (getLoadType().equalsIgnoreCase(RecordDispenserFactory.XML)){
  		if (location == null || location.trim().length() == 0){
  			starts.add(XML_NONE_LOCATION);
  			ends.add(XML_NONE_LOCATION);
  		} else {
  			starts.add(location);
  			ends.add(location);
  		}
  	}
  }
  
  /**
   * @param andTrim
   * @return
   */
  protected Class getHandlerClass(String name) {
    if (name == null || "".equals(name))
      return PassThroughHandler.class;
    try {
      return Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Configured handler " + name + " could not be loaded");
    }    
  }

  /**
   * @param defBuffer
   */
  private String getAndTrim(String key, Properties defBuffer) {
    String value = defBuffer.getProperty(key);
    if (value != null){
      value = value.trim();
    }
    return value;
  }


  /**
   * @return
   */
  public String getPostInsertUpdateName() {
    return destTable + "_POST_INSERT_UPDATE";
  }

  public String getRefreshQueryName() {
    return destTable + "_REFRESH";
  }

  /**
   * @return
   */
  public String getGeneratedKeyField() {
    return generatedKeyField;
  }
  
  public String getGeneratedKeySeqName(){
    return generatedKeySeqName;
  }

  public boolean hasPostInsertUpdate() {
    return postInsertUpdate != null;
  }

  public String getPostUpdateUpdateName(){
    return destTable + "_POST_UPDATE_UPDATE";
  }

  public String getPostUpdateInsertName(){
    return destTable + "_POST_UPDATE_INSERT";
  }

  public boolean hasPostUpdateUpdate() {
    return postUpdateUpdate != null;
  }

  public boolean hasPostUpdateInsert() {
    return postUpdateInsertion != null;
  }

  /**
   * @param index
   * @return
   */
  public boolean hasLookupInsert(int index) {
    return lookupInserts.get(index) != null;
  }
  
  public String getLookupInsertName(int index){
    return fields.get(index).toString() + "_INSERT_LOOKUP";
  }
  public String getLookupFullCacheQueryName(int index){
    return fields.get(index).toString() + "_FULL_CACHE_FOR_LOOKUP";
  }
  

  public String getLookupFullCacheResultName(int index) {
    return LOOKUP_VALUE_RESULT;
  }
  

  /**
   * @return
   */
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
  
  public int getFieldIndex(String field){
  	return fields.indexOf(field);
  }

  public String getDefaultValue(int fieldIndex){
  	return (defaultValues.get(fieldIndex) != null ? (String)defaultValues.get(fieldIndex) : null);
  }
  
  public String getUpdateMode() {
    return MIXED_MODE;
  }
  
  public String getLookupInsertKey(int fieldIndex){
  	return (String)lookupSelectKeys.get(fieldIndex);
  }
}
