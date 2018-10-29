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

import com.profitera.dc.InvalidLookupQueryException;
import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.impl.ILoaderOutput;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.GlobalFilter;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.dc.parser.impl.LookupDefinition;
import com.profitera.ibatis.SQLMapFileRenderer;
import com.profitera.util.Filter;
import com.profitera.util.MapCar;
import com.profitera.util.Strings;
import com.profitera.util.Utilities;
import com.profitera.util.reflect.Reflect;
import com.profitera.util.reflect.ReflectionException;

public class LoadingQueryWriter {

  private static final Class<?> MAP_IMPL_CLASS = HashMap.class;
  private static final String MAP_IMPL = MAP_IMPL_CLASS.getName();
  private static final String SELECT_ALL_CACHE_COUNT_PROP = "COUNT_ROW_EXISTS";
  private static final String SELECT_ALL_CACHE_STATEMENT_NAME = "FULL_CACHE_QUERY";

  public static final String TEMPLATE_FOOTER_PROP_NAME = "footer";
  public static final String TEMPLATE_HEADER_PROP_NAME = "header";
  public static final String TEMPLATE_KEY_KEY_START_PROP_NAME = "selectKeyStart";
  public static final String TEMPLATE_KEY_KEY_END_PROP_NAME = "selectKeyEnd";
  public static final String TEMPLATE_IS_PREFIX_SELECTKEY_NAME = "selectKeyIsPrefix";

  private String header;
  private String footer;
  private String selectKeyStart;
  private String selectKeyEnd;
  private boolean isPrefixSelectKey = true;

  final private LoadDefinition definition;
  final private Map<String, IFieldTextHandler> handlers = new HashMap<String, IFieldTextHandler>();

  public LoadingQueryWriter(LoadDefinition definition) {
    this.definition = definition;
    loadTemplate();
    List<FieldDefinition> fields = definition.getAllFields();
    for (int i = 0; i < fields.size(); i++) {
      FieldDefinition def = fields.get(i);
      String fieldName = def.getFieldName();
      String handlerClass = def.getHandler();
      String config = def.getHandlerArgs();
      try {
        IFieldTextHandler h = (IFieldTextHandler) Reflect.invokeConstructor(handlerClass, new Class[0], new Object[0]);
        h.configure(config);
        handlers.put(fieldName, h);
      } catch (ReflectionException e) {
        throw new RuntimeException("Unable to instantiate handler of type " + handlerClass);
      }
    }
  }

  public static String getSelectResultMapName(final String tableName) {
    return tableName + "_SELECT_MAP";
  }

  private static String getUpdateParamMapName(String tableName) {
    return "rmapname-" + tableName;
  }

  public static String getInsertParamMapName(String tableName) {
    return "insert-" + tableName;
  }

  public static String getSelectName(String destTable) {
    return destTable + "_SELECT";
  }

  public static String getSelectCountName(String destTable) {
    return getSelectName(destTable) + "_COUNT";
  }

  public static String getFullCacheQueryName(String destTable) {
    return destTable + "_" + SELECT_ALL_CACHE_STATEMENT_NAME;
  }

  public static String getFullCacheValueName() {
    return SELECT_ALL_CACHE_COUNT_PROP;
  }

  public static String getUpdateName(String destTable) {
    return destTable + "_UPDATE";
  }

  public static String getInsertName(String destTable) {
    return destTable + "_INSERT";
  }

  public static String getRefreshQueryName(String destTable) {
    return destTable + "_REFRESH";
  }

  public static String getPostUpdateUpdateName(String destTable, int index) {
    return destTable + "_POST_UPDATE_UPDATE_" + index;
  }

  public static String getPostUpdateInsertName(String destTable, int index) {
    return destTable + "_POST_UPDATE_INSERT_" + index;
  }

  public static String getPostInsertUpdateName(String destTable, int index) {
    return destTable + "_POST_INSERT_UPDATE_" + index;
  }

  public static String getPostInsertInsertName(String destTable, int index) {
    return destTable + "_POST_INSERT_INSERT_" + index;
  }

  public static String getLookupQueryName(String fieldName) {
    return fieldName + "_LOOKUP";
  }

  public static String getLookupInsertName(String fieldName) {
    return fieldName + "_INSERT_LOOKUP";
  }

  public static String getLookupFullCacheQueryName(String fieldName) {
    return fieldName + "_FULL_CACHE_FOR_LOOKUP";
  }

  public static String getLookupFullCacheResultName(String fieldName) {
    return fieldName + "_LOOKUP_VALUE_RESULT";
  }

  public static Properties getQueryTemplate() throws IOException {
    return Utilities.loadOrExit("querytemplate.properties");
  }

  private void loadTemplate() {
    try {
      Properties loadProps = getQueryTemplate();
      header = loadProps.getProperty(TEMPLATE_HEADER_PROP_NAME);
      footer = loadProps.getProperty(TEMPLATE_FOOTER_PROP_NAME);
      isPrefixSelectKey = loadProps.getProperty(TEMPLATE_IS_PREFIX_SELECTKEY_NAME, "T").toUpperCase().startsWith("T");
      selectKeyStart = loadProps.getProperty(TEMPLATE_KEY_KEY_START_PROP_NAME);
      if (selectKeyStart == null)
        selectKeyStart = "";
      selectKeyEnd = loadProps.getProperty(TEMPLATE_KEY_KEY_END_PROP_NAME);
      if (selectKeyEnd == null) {
        selectKeyEnd = "";
      } else if (selectKeyEnd.toLowerCase().startsWith("as") || selectKeyEnd.toLowerCase().startsWith("from")) {
        selectKeyEnd = " " + selectKeyEnd;
      }
    } catch (IOException e) {
      throw new RuntimeException("Templates required for loading configuration missing", e);
    }
  }

  public void writeQueriesToBuffer(ILoaderOutput output, final BufferedWriter xmlBuffer) throws IOException,
      InvalidLookupQueryException {
    xmlBuffer.write(header + "\n");
    writeQueries(xmlBuffer, definition.getDestTable(), Arrays.asList(definition.getAllFieldName()),
        Arrays.asList(definition.getAllKeyName()));
    output.writeOutputSpecificSql(xmlBuffer);
    xmlBuffer.write(footer);
  }

  private void writeQueries(final BufferedWriter xmlBuffer, final String tableName, final List fields,
      final List keyFields) throws IOException, InvalidLookupQueryException {
    writeResultMap(xmlBuffer, tableName, fields);
    writeSelectQuery(xmlBuffer, tableName, keyFields, fields);
    writeSelectCountQuery(xmlBuffer, tableName, keyFields);
    writeSelectAllCacheQuery(xmlBuffer, tableName, keyFields);
    writeUpdateQuery(xmlBuffer, tableName, keyFields, fields);
    writeInsertQuery(xmlBuffer, tableName, fields);
    if (definition.isRefreshData()) {
      writeRefreshQuery(xmlBuffer, tableName);
    }
    if (definition.getPostInsertUpdateQueries() != null) {
      String[] queries = definition.getPostInsertUpdateQueries();
      for (int i = 0; i < queries.length; i++) {
        writePostQuery(xmlBuffer, getPostInsertUpdateName(tableName, i), queries[i]);
      }
    }
    if (definition.getPostInsertInsertQueries() != null) {
      String[] queries = definition.getPostInsertInsertQueries();
      for (int i = 0; i < queries.length; i++) {
        writePostQuery(xmlBuffer, getPostInsertInsertName(tableName, i), queries[i]);
      }
    }
    if (definition.getPostUpdateUpdateQueries() != null) {
      String[] queries = definition.getPostUpdateUpdateQueries();
      for (int i = 0; i < queries.length; i++) {
        writePostQuery(xmlBuffer, getPostUpdateUpdateName(tableName, i), queries[i]);
      }
    }
    if (definition.getPostUpdateInsertQueries() != null) {
      String[] queries = definition.getPostUpdateInsertQueries();
      for (int i = 0; i < queries.length; i++) {
        writePostQuery(xmlBuffer, getPostUpdateInsertName(tableName, i), queries[i]);
      }
    }
    writeLookupQueries(xmlBuffer, fields, definition.getGlobalFilters());
  }

  private void writeResultMap(final BufferedWriter xmlBuffer, final String tableName, final List fields)
      throws IOException {
    // <resultMap id="PTRCUSTOMER_SELECT_MAP" class="java.util.HashMap">
    // <result property="accountId" column="ACCOUNT_ID" javaType="double"/>
    // <result property="cycDelId" column="CYC_DEL_ID" javaType="double"/>
    // </resultMap>
    xmlBuffer.write("<resultMap id=" + Strings.quoteString(getSelectResultMapName(tableName)));
    xmlBuffer.write(" class=" + Strings.quoteString(MAP_IMPL) + ">\n");
    List<String> updateFields = new RemoveExternalFilter().filterItems(fields);
    for (Iterator i = updateFields.iterator(); i.hasNext();) {
      String field = (String) i.next();
      xmlBuffer.write("<result property=" + Strings.quoteString(field) + " column=" + Strings.quoteString(field)
          + "/>\n"); // javaType="double"
    }
    xmlBuffer.write("</resultMap>\n");
    //
    xmlBuffer.write("<parameterMap id=" + Strings.quoteString(getUpdateParamMapName(tableName)));
    xmlBuffer.write(" class=" + Strings.quoteString(Map.class.getName()) + ">\n");
    for (Iterator<String> i = updateFields.iterator(); i.hasNext();) {
      String fieldName = i.next();
      FieldDefinition field = definition.getField(fieldName);
      if (field.isKey())
        continue;
      Class valueType = handlers.get(fieldName).getValueType();
      String jdbcType = getJDBCType(valueType);
      xmlBuffer.write("<parameter property=" + Strings.quoteString(fieldName) + " javaType="
          + Strings.quoteString(valueType.getName()) + " jdbcType=" + Strings.quoteString(jdbcType) + "/>\n"); // javaType="double"
    }
    for (Iterator<String> i = Arrays.asList(definition.getAllKeyName()).iterator(); i.hasNext();) {
      String fieldName = (String) i.next();
      Class valueType = handlers.get(fieldName).getValueType();
      String jdbcType = getJDBCType(valueType);
      xmlBuffer.write("<parameter property=" + Strings.quoteString(fieldName) + " javaType="
          + Strings.quoteString(valueType.getName()) + " jdbcType=" + Strings.quoteString(jdbcType) + "/>\n"); // javaType="double"
    }
    xmlBuffer.write("</parameterMap>\n");
    //
    xmlBuffer.write("<parameterMap id=" + Strings.quoteString(getInsertParamMapName(tableName)));
    xmlBuffer.write(" class=" + Strings.quoteString(Map.class.getName()) + ">\n");
    List<String> insertFields = updateFields;
    for (Iterator<String> i = insertFields.iterator(); i.hasNext();) {
      String fieldName = i.next();
      Class valueType = handlers.get(fieldName).getValueType();
      String jdbcType = getJDBCType(valueType);
      xmlBuffer.write("<parameter property=" + Strings.quoteString(fieldName) + " javaType="
          + Strings.quoteString(valueType.getName()) + " jdbcType=" + Strings.quoteString(jdbcType) + "/>\n"); // javaType="double"
    }
    // Add the generated parameter map item only if it was not already defined
    // as
    // a column in the table for some other purpose.
    if (definition.isGenerateKey() && !insertFields.contains(definition.getGeneratedKeyColumn())) {
      FieldDefinition genKey = definition.getField(definition.getGeneratedKeyColumn());
      if (genKey == null || (genKey != null && !genKey.isExternal())) {
        String jdbcType = getJDBCType(Long.class);
        xmlBuffer.write("<parameter property=" + Strings.quoteString(definition.getGeneratedKeyColumn()) + " javaType="
            + Strings.quoteString(Long.class.getName()) + " jdbcType=" + Strings.quoteString(jdbcType) + "/>\n"); //
      }
    }
    xmlBuffer.write("</parameterMap>\n");

  }

  private void writeSelectQuery(BufferedWriter xmlBuffer, String tableName, List keyFields, final List fields)
      throws IOException {
    xmlBuffer.write("<select id=" + Strings.quoteString(getSelectName(tableName)));
    xmlBuffer.write(" resultMap=" + Strings.quoteString(getSelectResultMapName(tableName)));
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n  <![CDATA[\n");
    xmlBuffer.write("    select " + Strings.getListString(new RemoveExternalFilter().filterItems(fields), ", "));
    xmlBuffer.write("    from " + tableName + "\n");
    xmlBuffer.write(getWhereClause(keyFields, false));
    xmlBuffer.write("\n  ]]>\n</select>\n");
  }

  private String getWhereClause(List keyFields, final boolean useQs) {
    if (definition.getSelectWhereClause() != null) {
      return "   " + definition.getSelectWhereClause();
    }
    if (keyFields.isEmpty())
      return "";
    return "    where " + Strings.getListString(MapCar.map(new MapCar() {
      public Object map(Object o) {
        if (useQs)
          return o.toString() + "= ?"; // " = #" + o.toString() + "#";
        else
          return o.toString() + " = #" + o.toString() + "#";
      }
    }, keyFields), "\n      and ");
  }

  private void writeSelectCountQuery(BufferedWriter xmlBuffer, String tableName, List keyFields) throws IOException {
    xmlBuffer.write("<select id=" + Strings.quoteString(getSelectCountName(tableName)));
    xmlBuffer.write(" resultClass=" + Strings.quoteString("long"));
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n  <![CDATA[\n");
    xmlBuffer.write("    select count(*)");
    xmlBuffer.write("    from " + tableName + "\n");
    xmlBuffer.write(getWhereClause(keyFields, false));
    xmlBuffer.write("\n  ]]>\n</select>\n");
  }

  private void writeSelectAllCacheQuery(BufferedWriter xmlBuffer, String tableName, List keyFields) throws IOException {
    String sql = "select 1";
    for (Iterator i = keyFields.iterator(); i.hasNext();) {
      String f = (String) i.next();
      sql = sql + ", " + f;
    }
    sql = sql + " from " + tableName;
    SQLMapFileRenderer r = new SQLMapFileRenderer();
    String[] props = new String[keyFields.size() + 1];
    Class[] javaTypes = new Class[props.length];
    props[0] = getFullCacheValueName();
    javaTypes[0] = Long.class;
    for (int i = 1; i < props.length; i++) {
      String field = (String) keyFields.get(i - 1);
      props[i] = field;
      javaTypes[i] = getLookupClassForField(field);
    }
    String rmapName = getFullCacheQueryName(tableName) + "-rmap";
    xmlBuffer.write(r.renderResultMap(rmapName, MAP_IMPL_CLASS, props, javaTypes));
    xmlBuffer.write(r.renderSelect(getFullCacheQueryName(tableName), rmapName, sql));
  }

  private void writeUpdateQuery(BufferedWriter xmlBuffer, String tableName, List keyFields, List fields)
      throws IOException {
    xmlBuffer.write("<update id=" + Strings.quoteString(getUpdateName(tableName)));
    xmlBuffer.write(" parameterMap=" + Strings.quoteString(getUpdateParamMapName(tableName)));
    xmlBuffer.write(">\n  <![CDATA[\n");
    xmlBuffer.write("    update " + tableName + " set " + Strings.getListString(MapCar.map(new MapCar() {
      public Object map(Object o) {
        return o.toString() + "= ?";
      }
    }, new RemoveKeysFilter().filterItems(new RemoveExternalFilter().filterItems(fields))), ",\n      "));
    xmlBuffer.write(getUpdateWhereClause(keyFields, true));
    xmlBuffer.write("\n ]]>\n</update>\n");
  }

  private String getUpdateWhereClause(List keyFields, final boolean useQs) {
    if (definition.getUpdateWhereClause() != null) {
      return "   " + definition.getUpdateWhereClause();
    }
    return "    where " + Strings.getListString(MapCar.map(new MapCar() {
      public Object map(Object o) {
        if (useQs)
          return o.toString() + "= ?"; // " = #" + o.toString() + "#";
        else
          return o.toString() + " = #" + o.toString() + "#";
      }
    }, keyFields), "\n      and ");
  }

  private void writeInsertQuery(BufferedWriter xmlBuffer, String tableName, List fields) throws IOException {
    xmlBuffer.write("<insert id=" + Strings.quoteString(getInsertName(tableName)));
    xmlBuffer.write(" parameterMap=" + Strings.quoteString(getInsertParamMapName(tableName)));
    xmlBuffer.write(">\n");
    String generatedKey = definition.getGeneratedKeyColumn();
    if (definition.isGenerateKey() && isPrefixSelectKey) {
      writeSelectKey(xmlBuffer, tableName, generatedKey);
    }
    List fieldsPlusGenKey = getFieldsPlusGenKey(fields, generatedKey);
    xmlBuffer.write("<![CDATA[\n");
    xmlBuffer.write("    insert into " + tableName + "\n");
    xmlBuffer.write("    (" + Strings.getListString(fieldsPlusGenKey, ", ") + ")");
    xmlBuffer.write("    values (" + Strings.getListString(MapCar.map(new MapCar() {
      public Object map(Object o) {
        return "?";
      }
    }, fieldsPlusGenKey), ", "));
    xmlBuffer.write(")");
    xmlBuffer.write("\n  ]]>\n");
    if (definition.isGenerateKey() && !isPrefixSelectKey) {
      writeSelectKey(xmlBuffer, tableName, generatedKey);
    }
    xmlBuffer.write("</insert>\n");
  }

  private void writeSelectKey(BufferedWriter xmlBuffer, String destTable, String generatedKeyField) throws IOException {
    String sequenceName = definition.getGenerateKeySeq().trim();
    String q = getSequenceSelect(sequenceName);
    writeSelectKey(generatedKeyField, q, "long", xmlBuffer);
  }

  private void writeSelectKey(String generatedKeyField, String query, String dataType, BufferedWriter xmlBuffer)
      throws IOException {
    xmlBuffer.write("<selectKey resultClass=" + Strings.quoteString(dataType) + " keyProperty="
        + Strings.quoteString(generatedKeyField) + ">\n");
    xmlBuffer.write("      " + query + "\n");
    xmlBuffer.write("</selectKey>\n");
  }

  private List<String> getFieldsPlusGenKey(List<String> fields, String generatedKeyField) {
    List<String> fieldsPlusGenKey = new ArrayList<String>(fields);
    fieldsPlusGenKey = new RemoveExternalFilter().filterItems(fieldsPlusGenKey);
    // Make sure we do not re-add the generated column if it was already defined
    // for some other use.
    if (definition.isGenerateKey() && !fieldsPlusGenKey.contains(definition.getGeneratedKeyColumn())) {
      fieldsPlusGenKey.add(generatedKeyField);
    }
    return fieldsPlusGenKey;
  }

  private void writeRefreshQuery(BufferedWriter xmlBuffer, String tableName) throws IOException {
    String refreshSql = null;
    long refreshDataQueryTimeout = definition.getRefreshTimeout();
    String refreshQuery = definition.getRefreshDataQuery();
    if (refreshQuery == null) {
      refreshSql = "<delete id=" + Strings.quoteString(getRefreshQueryName(tableName));
      if (refreshDataQueryTimeout >= 0)
        refreshSql += " timeout=" + Strings.quoteString("" + refreshDataQueryTimeout);
      refreshSql += ">\n";
      refreshSql += "\tDELETE FROM " + tableName.toUpperCase() + "\n";
      refreshSql += "</delete>";
    } else if (refreshQuery.trim().toLowerCase().startsWith("delete")) {
      refreshSql = "<delete id=" + Strings.quoteString(getRefreshQueryName(tableName));
      if (refreshDataQueryTimeout >= 0)
        refreshSql += " timeout=" + Strings.quoteString("" + refreshDataQueryTimeout);
      refreshSql += ">\n";
      refreshSql += "\t<![CDATA[" + refreshQuery + "]]>\n";
      refreshSql += "</delete>";
    } else {
      refreshSql = "<update id=" + Strings.quoteString(getRefreshQueryName(tableName));
      if (refreshDataQueryTimeout >= 0)
        refreshSql += " timeout=" + Strings.quoteString("" + refreshDataQueryTimeout);
      refreshSql += ">\n";
      refreshSql += "\t<![CDATA[" + refreshQuery + "]]>\n";
      refreshSql += "</update>";
    }
    xmlBuffer.write(refreshSql);
  }

  private void writePostQuery(BufferedWriter xmlBuffer, String name, String query) throws IOException {
    xmlBuffer.write("<update id=" + Strings.quoteString(name));
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n  <![CDATA[\n");
    xmlBuffer.write(query);
    xmlBuffer.write("\n  ]]>\n</update>\n");
  }

  private void writeLookupQueries(BufferedWriter xmlBuffer, List<String> fields, GlobalFilter[] filters) throws IOException,
      InvalidLookupQueryException {
    for (int i = 0; i < fields.size(); i++) {
      String fieldName = fields.get(i);
      FieldDefinition fd = definition.getField(fieldName);
      if (!fd.isLookupField()) {
        continue;
      }
      LookupDefinition ld = fd.getLookupDefinition();
      String type = handlers.get(fieldName).getValueType().getName();
      writeLookupQuery(getLookupQueryName(fieldName), ld.getLookupQuery(), type, xmlBuffer);
      if (ld.getLookupInsertQuery() != null) {
        writeLookupInsert(getLookupInsertName(fieldName), ld.getLookupInsertGenKey(), ld.getLookupInsertQuery(), type,
            xmlBuffer);
      }
      if (ld.getLookupFullCacheQuery() != null) {
        writeLookupFullCacheQuery(fieldName, getLookupFullCacheQueryName(fieldName), ld.getLookupFullCacheQuery(),
            type, Arrays.asList(ld.getLookupQueryParams()), xmlBuffer);
      }
    }
    for (int i = 0; i < filters.length; i++) {
      LookupDefinition ld = filters[i].getLookup();
      String type = Boolean.class.getName();
      writeLookupQuery(getLookupQueryName(filters[i].getName()), ld.getLookupQuery(), type, xmlBuffer);
    }
  }

  private void writeLookupQuery(String name, String sqltext, String dataType, BufferedWriter xmlBuffer)
      throws IOException {
    xmlBuffer.write("<select id=" + Strings.quoteString(name));
    xmlBuffer.write(" resultClass=" + Strings.quoteString(dataType));
    // jambu changed this from String.getClass().getName() to MAP_IMPL
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n  <![CDATA[\n");
    xmlBuffer.write(sqltext);
    xmlBuffer.write("\n  ]]>\n</select>\n");
  }

  private void writeLookupInsert(String lookupInsertName, String selectKey, String lookupInsert, String dataType,
      BufferedWriter xmlBuffer) throws IOException {
    xmlBuffer.write("<insert id=" + Strings.quoteString(lookupInsertName));
    // jambu changed this from Map.getClass().getName() to MAP_IMPL
    xmlBuffer.write(" parameterClass=" + Strings.quoteString(MAP_IMPL));
    xmlBuffer.write(">\n");
    if (selectKey != null && isPrefixSelectKey) {
      writeSelectKey("ID", selectKey, dataType, xmlBuffer);
    }
    xmlBuffer.write("<![CDATA[\n");
    xmlBuffer.write(lookupInsert + "\n");
    xmlBuffer.write("\n  ]]>\n");
    if (selectKey != null && !isPrefixSelectKey) {
      writeSelectKey("ID", selectKey, dataType, xmlBuffer);
    }
    xmlBuffer.write("</insert>\n");
  }

  private void writeLookupFullCacheQuery(String fieldName, String queryName, String sqltext, String dataType,
      List lookupParameters, BufferedWriter xmlBuffer) throws IOException {
    SQLMapFileRenderer r = new SQLMapFileRenderer();
    if (lookupParameters == null)
      lookupParameters = java.util.Collections.EMPTY_LIST;
    String[] props = new String[lookupParameters.size() + 1];
    Class[] classes = new Class[props.length];
    props[0] = getLookupFullCacheResultName(fieldName);
    try {
      classes[0] = Class.forName(dataType);
    } catch (ClassNotFoundException e) {
      // Unreachable
    }
    for (int i = 1; i < props.length; i++) {
      props[i] = (String) lookupParameters.get(i - 1);
      FieldDefinition propsField = definition.getField(props[i]);
      // If the lookup param is the field itself it has to map as a string.
      // B/c it it coming from the file to be looked up by this query, duh!
      if (!props[i].equals(fieldName) && propsField != null && propsField.isLookupField()) {
        Class c = handlers.get(props[i]).getValueType();
        classes[i] = c;
      } else {
        classes[i] = String.class;
      }
    }
    xmlBuffer.write(r.renderResultMap(queryName + "-rmap", MAP_IMPL_CLASS, props, classes));
    xmlBuffer.write(r.renderSelect(queryName, queryName + "-rmap", sqltext));
  }

  private Class getLookupClassForField(String fieldName) {
    FieldDefinition field = definition.getField(fieldName);
    if (field == null)
      return String.class;
    return handlers.get(fieldName).getValueType();
  }

  private String getJDBCType(Class<?> valueType) {
    String jdbcType = "VARCHAR";
    if (valueType.getSuperclass().equals(Number.class))
      jdbcType = "NUMERIC";
    else if (valueType.equals(Date.class))
      jdbcType = "TIMESTAMP";
    else if (valueType.equals(java.sql.Date.class))
      jdbcType = "DATE";
    else if (valueType.equals(Time.class))
      jdbcType = "TIME";
    return jdbcType;
  }

  private final class RemoveExternalFilter extends Filter {
    protected boolean filter(Object o) {
      FieldDefinition field = definition.getField((String) o);
      if (field == null)
        return true;
      return !field.isExternal();
    }
  }

  public class RemoveKeysFilter extends Filter {
    protected boolean filter(Object o) {
      FieldDefinition field = definition.getField((String) o);
      if (field == null)
        return true;
      return !field.isKey();
    }

  }

  public String getSequenceSelect(String sequenceName) {
    return selectKeyStart + sequenceName + selectKeyEnd;
  }
}
