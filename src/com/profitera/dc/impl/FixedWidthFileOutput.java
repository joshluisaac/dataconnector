package com.profitera.dc.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.ibatis.sqlmap.engine.mapping.parameter.InlineParameterMapParser;
import com.ibatis.sqlmap.engine.mapping.parameter.ParameterMapping;
import com.ibatis.sqlmap.engine.mapping.sql.SqlText;
import com.ibatis.sqlmap.engine.type.TypeHandlerFactory;
import com.profitera.dc.ErrorSummary;
import com.profitera.dc.InvalidLookupQueryException;
import com.profitera.dc.LoadingErrorList;
import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.handler.IntegerHandler;
import com.profitera.dc.handler.PassThroughHandler;
import com.profitera.dc.parser.V2LoadDefinitionParser;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.dc.parser.impl.Location;
import com.profitera.util.Filter;
import com.profitera.util.MapCar;
import com.profitera.util.Strings;
import com.profitera.util.io.FileUtil;
import com.profitera.util.reflect.ReflectionException;
import com.profitera.util.xml.DocumentLoader;

public class FixedWidthFileOutput extends AbstractLoaderOutput implements ILoaderOutput {
  private static final Charset UTF8 = Charset.forName("UTF8");
  private static final byte[] NEW_LINE = System.getProperty("line.separator").getBytes(UTF8);
  private static final FieldDefinition LINE_NO = new FieldDefinition("LINE_NO", new Location("0", "0"), false);
  private static final IFieldTextHandler PASS_THROUGH = new PassThroughHandler();
  private static final String CUR = "c_LOAD";
  private File outputFile;
  private int maxLine = 0;
  private boolean isIterativeMode = true;
  private final FieldTypeInfoRegistry types;

  static {
    LINE_NO.setHandler(IntegerHandler.class.getName(), "");
  }
  private int getFieldValueWidth(FieldDefinition field, IFieldTextHandler handler) {
    IFieldTypeInfo info = types.getHandlerTypeInfo(handler);
    if (isDeferLookupResolution(field)) {
      info = types.getHandlerTypeInfo(PASS_THROUGH);
    }
    return info.getFieldTextWidth(field, handler);
  }

  @Override
  public boolean isDeferLookupResolution(FieldDefinition field) {
    return field.isLookupField() && !field.getLookupDefinition().isLookupCache();
  }

  public FixedWidthFileOutput(String name, LoadDefinition d) {
    super(name, d);
    types = new FieldTypeInfoRegistry(d);
  }

  @Override
  public void generateOutput(final SqlMapClient writer, final List<String> linesRead, long startLineNo,
      final Map<String, IFieldTextHandler> handlers, ErrorSummary errorSummary, final File sourceFile,
      final List<String> badLines, final List<Exception> exceptions, final List<String> statementNames,
      final List<Map<String, Object>> statementArgs) throws SQLException {
    try {
      for (int i = 0; i < statementArgs.size(); i++) {
        writeToFile(statementArgs.get(i), (int) startLineNo + i, sourceFile, handlers);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      errorSummary.setFailedLines(badLines);
    } catch (IOException e) {
      getLog().emit(DataLoaderLogClient.BAD_WRITE_FAIL, e, getLoadName());
    }
    try {
      errorSummary.setExceptions(exceptions);
    } catch (IOException e) {
      getLog().emit(DataLoaderLogClient.TRACE_WRITE_FAIL, e, getLoadName());
    }
  }

  public void executeUpdates(final SqlMapClient client, final String statementName,
      final Map<String, Object> statementArgs, int line, File sourceFile, Map<String, IFieldTextHandler> handlers) {
    try {
      writeToFile(statementArgs, line, sourceFile, handlers);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Object fixedWidthFileLock = new Object();
  private OutputStream fw;
  private String fileOutputPath;
  protected File getOutputFile() {
    return outputFile;
  }

  private void writeToFile(Map<String, Object> statementArgs, int line, File sourceFile,
      Map<String, IFieldTextHandler> handlers) throws IOException {
    synchronized (fixedWidthFileLock) {
      if (fw == null) {
        File dir = sourceFile.getParentFile();
        if (getFileOutputPath() != null && getFileOutputPath().length() > 0) {
          dir = new File(getFileOutputPath());
          if (!dir.exists()) {
            dir.mkdirs();
          }
          if (!dir.exists()) {
            throw new IllegalArgumentException("Unable to create directory " + getFileOutputPath() + "(" + FileUtil.tryCanonical(dir) + ") for " + getLoadName());
          }
        }
        outputFile = new File(dir, sourceFile.getName() + ".asc");
        fw = getFileManager().getOutputStream(outputFile);
      }
      List<FieldDefinition> allFields = getDefinition().getAllFields();
      StringBuilder sb = new StringBuilder();
      sb.append(Strings.leftPad(line + "", types.getIntegerWidth(), '0'));
      if (maxLine < line) {
        maxLine = line;
      }
      for (FieldDefinition fieldDefinition : allFields) {
        Object value = statementArgs.get(fieldDefinition.getFieldName());
        IFieldTextHandler handler = handlers.get(fieldDefinition.getFieldName());
        Class<?> valueType = handler.getValueType();
        int totalWidth = getFieldValueWidth(fieldDefinition, handler);
        if (isDeferLookupResolution(fieldDefinition)) {
          valueType = PASS_THROUGH.getValueType();
        }
        if (value == null) {
          sb.append(Strings.pad("Y", totalWidth+1));
        } else {
          sb.append("N");
          IFieldTypeInfo info = types.getTypeInfo(valueType);
          sb.append(info.render(value, totalWidth));
        }
      }
      fw.write(sb.toString().getBytes(UTF8));
      fw.write(NEW_LINE);
    }
  }


  private class LoadLocation {
    private int nullInd;
    private int end;
    private int start;
    private final String fieldDef;

    public LoadLocation(String fieldDef, int start, int end, int nullInd) {
      this.fieldDef = fieldDef;
      this.start = start;
      this.end = end;
      this. nullInd = nullInd;
    }
    public String toString() {
      return start + "-" + end + "(" + nullInd + ")";
    }
  }
  private String getColumnDefinition(FieldDefinition d) throws ReflectionException {
    IFieldTextHandler handler = types.getHandler(d);
    int width = getFieldValueWidth(d, handler);
    IFieldTypeInfo iFieldTypeInfo = types.getHandlerTypeInfo(handler);
    return iFieldTypeInfo.getDatabaseColumnDefinition(d, width);
  }


  private List<LoadLocation> getColumnDefinitions() throws ReflectionException {
    List<FieldDefinition> allFields = getDefinition().getAllFields();
    List<LoadLocation> locations = new ArrayList<LoadLocation>();
    IFieldTypeInfo info = types.getHandlerTypeInfo(new IntegerHandler());
    locations.add(new LoadLocation(" " + info.getDatabaseColumnDefinition(LINE_NO, 0), 1, getFieldValueWidth(LINE_NO, types.getHandler(LINE_NO)), 0));
    for (FieldDefinition d : allFields) {
      IFieldTextHandler handler = types.getHandler(d);
      int width = getFieldValueWidth(d, handler);
      int lastLocationEnd = locations.get(locations.size() - 1).end;
      IFieldTypeInfo iFieldTypeInfo = types.getHandlerTypeInfo(handler);
      if (isDeferLookupResolution(d)) {
        iFieldTypeInfo = types.getHandlerTypeInfo(PASS_THROUGH);
      }
      LoadLocation location = new LoadLocation(" " + iFieldTypeInfo.getDatabaseColumnDefinition(d, width), lastLocationEnd + 2, lastLocationEnd + 1 + width, lastLocationEnd + 1);
      locations.add(location);
    }
    return locations;
  }

  public String getIntermediateTableDefinition() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      PrintStream ps = new PrintStream(out);
      List<LoadLocation> locations = getColumnDefinitions();
      List<String> columnDefinitions = new ArrayList<String>();
      for (LoadLocation l : locations) {
        columnDefinitions.add(l.fieldDef);
      }
      columnDefinitions.add(" LOADED VARCHAR(1) NOT NULL DEFAULT 'N'");
      columnDefinitions.add(" L_ROW_ID BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL");
      ps.println("CREATE TABLE " + getLoadBufferTableName() + " (");
      ps.println(Strings.getListString(columnDefinitions, ",\n"));
      ps.println(")");
    } catch (ReflectionException e){}
    return new String(out.toByteArray(), UTF8);
  }
  public String getNativeLoadCommand(boolean isFileParameter) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
    PrintStream ps = new PrintStream(out);
    List<LoadLocation> locations = getColumnDefinitions();
    List<String> startEnd = getFileLocations(locations);
    List<Integer> nullIndicators = getNullIndicatorLocations(locations);
    String file = "source.asc";
    if (isFileParameter) {
      file = "$FILE$";
    }
    //yyyy-MM-dd-HH.mm.ss.SSSSSS
    ps.println("CALL SYSPROC.ADMIN_CMD('load from " + file + " of ASC modified by timestampformat=\"YYYY-MM-DD-HH.MM.SS.UUUUUU\" dateformat=\"YYYY-MM-DD\" method L ("
      + Strings.getListString(startEnd, ", ")
      + ") NULL INDICATORS (" + Strings.getListString(nullIndicators, ", ") + ")"
      + " MESSAGES ON SERVER REPLACE into " + getLoadBufferTableName() + "')");
    ps.close();
    } catch (ReflectionException e){}
    return new String(out.toByteArray(), UTF8);
  }

  protected String getLoadBufferTableName() {
    return "L_" + getDefinition().getDestTable();
  }

  public String getAlternateStoredProcedure() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      PrintStream ps = new PrintStream(out);
      ps.println("CREATE PROCEDURE " + getStoredProcedureName() + "()");
      ps.println("LANGUAGE SQL");
      ps.println("BEGIN");
      ps.println("DECLARE v_LAST_LINE BIGINT;");
      ps.println("DECLARE v_CURRENT_LINE BIGINT DEFAULT 0;");
      ps.println("DECLARE v_COMMIT_SIZE BIGINT DEFAULT " + getCommitSize() + ";");
      ps.println("DECLARE v_LINE_NO BIGINT;");
      List<FieldDefinition> fields = getDefinition().getAllFields();
      for (FieldDefinition d : fields) {
        ps.format("DECLARE v_%s;\n", getColumnDefinition(d));
      }
      if (getDefinition().isGenerateKey() && !getDefinition().isGeneratedKeyFieldAlsoDefined()) {
        ps.format("DECLARE v_%s BIGINT;\n", getDefinition().getGeneratedKeyColumn());
      }
      String alias = "S";
      ps.println("DECLARE v_TARGET_ROW_PRESENT BIGINT;");
      ps.println("DECLARE SQLCODE INT DEFAULT 0;");
      ps.println("DECLARE v_FETCH_OK INT DEFAULT 0;");
      ps.format("DECLARE %s CURSOR FOR SELECT %s.LINE_NO,", CUR, alias);
      ps.println(getLoadSelectColumnsAliased(alias));
      if (getDefinition().getUpdateMode().equals(LoadDefinition.MIXED_MODE)) {
        if (getDefinition().getAllKeys().length == 0) {
          throw new IllegalArgumentException("Unable to generate mixed-mode stored procedure, no keys defined");
        }
        ps.println(", case when target." + getDefinition().getAllKeyName()[0] + " is null then 0 else 1 end as TARGET_ROW_PRESENT");
      } else if (getDefinition().getUpdateMode().equals(LoadDefinition.INSERT_MODE)) {
        ps.println(", 0 as TARGET_ROW_PRESENT");
      } else { // UPDATE_MODE
        ps.println(", 1 as TARGET_ROW_PRESENT");
      }
      ps.format("from %s %s\n", getLoadBufferTableName(), alias);
      ps.print(getLoadFromClauseLookupEntries(alias));
      if (getDefinition().getUpdateMode().equals(LoadDefinition.MIXED_MODE)) {
        ps.format("left outer join %s target on %s\n", getDefinition().getDestTable(), getAliasedKeyJoin("target", alias));
      }
      ps.format("where %s.LINE_NO > v_CURRENT_LINE order by %s.LINE_NO fetch first 50 rows only;\n", alias, alias, alias);
      ps.format("SET v_LAST_LINE = (Select MAX(LINE_NO) from %s);\n", getLoadBufferTableName());
      ps.println("WHILE (v_CURRENT_LINE < v_LAST_LINE) DO");
      ps.format("OPEN %s;\n", CUR);
      String intoList = getCursorFetchIntoList();
      ps.format("FETCH FROM %s INTO %s;\n", CUR, intoList);
      ps.println("set v_FETCH_OK = SQLCODE;");
      ps.println("IF (NOT v_FETCH_OK = 0) THEN SET v_CURRENT_LINE = v_LAST_LINE; END IF;");
      ps.println("WHILE (v_FETCH_OK = 0) DO");
      ps.println("SET v_CURRENT_LINE = v_LINE_NO;");
      if (getDefinition().getUpdateMode().equals(LoadDefinition.MIXED_MODE)) {
        ps.println("CASE v_TARGET_ROW_PRESENT");
        ps.println("WHEN 1 THEN");
      }
      if (!getDefinition().getUpdateMode().equals(LoadDefinition.INSERT_MODE)) {
        ps.format(" update %s set %s %s;\n", getDefinition().getDestTable(), getDestinationVariableUpdateAssignments(), getUpdateWhereClause());
        ps.format(" update %s set LOADED = 'U' where LINE_NO = v_LINE_NO;\n", getLoadBufferTableName());
        if (getDefinition().getPostUpdateInsertQueries() != null) {
          String[] statements = getDefinition().getPostUpdateInsertQueries();
          for (int i = 0; i < statements.length; i++) {
            ps.println(getVariableInsertVersionOfSql(statements[i]));
          }
        }
        if (getDefinition().getPostUpdateUpdateQueries() != null) {
          String[] statements = getDefinition().getPostUpdateUpdateQueries();
          for (int i = 0; i < statements.length; i++) {
            ps.println(getVariableInsertVersionOfSql(statements[i]));
          }
        }
      }
      if (getDefinition().getUpdateMode().equals(LoadDefinition.MIXED_MODE)) {
        ps.println("ELSE");
      }
      if (!getDefinition().getUpdateMode().equals(LoadDefinition.UPDATE_MODE)) {
        ps.format(" update %s set LOADED = 'I' where LINE_NO = v_LINE_NO;\n", getLoadBufferTableName());
        String insertColumnsPrefix = "";
        String insertValuesPrefix = "";
        if (getDefinition().isGenerateKey()) {
          ps.format(" SET v_%s = nextval for %s;\n", getDefinition().getGeneratedKeyColumn(), getDefinition().getGenerateKeySeq());
          if (!getDefinition().isGeneratedKeyFieldAlsoDefined()) {
            insertValuesPrefix = "v_" + getDefinition().getGeneratedKeyColumn() + ", ";
            insertColumnsPrefix = getDefinition().getGeneratedKeyColumn() + ", ";
          }
        }
        ps.format(" insert into %s (%s%s)\n      values (%s%s);\n", getDefinition().getDestTable(), insertColumnsPrefix, 
            getDestinationInsertColumnList(), insertValuesPrefix, getInsertVariableList());
        if (getDefinition().getPostInsertInsertQueries() != null) {
          String[] statements = getDefinition().getPostInsertInsertQueries();
          for (int i = 0; i < statements.length; i++) {
            ps.println(getVariableInsertVersionOfSql(statements[i]));
          }
          
        }
        if (getDefinition().getPostInsertUpdateQueries() != null) {
          String[] statements = getDefinition().getPostInsertUpdateQueries();
          for (int i = 0; i < statements.length; i++) {
            ps.println(getVariableInsertVersionOfSql(statements[i]));
          }
        }
      }
      if (getDefinition().getUpdateMode().equals(LoadDefinition.MIXED_MODE)) {
        ps.println("END CASE;");
      }
      ps.format("FETCH FROM %s INTO %s;\n", CUR, intoList);
      ps.println("set v_FETCH_OK = SQLCODE;");
      ps.println("END WHILE;");
      ps.format("CLOSE %s;\n", CUR);
      ps.println("COMMIT;");
      
      ps.println("END WHILE;");
      ps.println("END");
      //ps.println("@");
      ps.close();
    } catch (Throwable t) {
      t.printStackTrace();
      return "";
    }
    return new String(out.toByteArray(), UTF8);
  }

  public String getStoredProcedure() {
    return getOriginalStoredProcedure();
  }
  public String getOriginalStoredProcedure() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      PrintStream ps = new PrintStream(out);
      if (isIterativeMode()) {
        ps.println("CREATE PROCEDURE " + getStoredProcedureName() + "(IN p_START_LINE BIGINT, IN p_END_LINE BIGINT)");
      } else {
        ps.println("CREATE PROCEDURE " + getStoredProcedureName() + "()");
      }
      
      ps.println("LANGUAGE SQL");
      ps.println("BEGIN");
      ps.println("DECLARE v_LAST_LINE BIGINT;");
      ps.println("DECLARE v_CURRENT_LINE BIGINT DEFAULT 1;");
      ps.println("DECLARE v_COMMIT_SIZE BIGINT DEFAULT " + getCommitSize() + ";");
      ps.println("DECLARE v_LINE_NO BIGINT;");
      List<FieldDefinition> fields = getDefinition().getAllFields();
      for (FieldDefinition d : fields) {
        ps.format("DECLARE v_%s;\n", getColumnDefinition(d));
      }
      if (getDefinition().isGenerateKey() && !getDefinition().isGeneratedKeyFieldAlsoDefined()) {
        ps.format("DECLARE v_%s BIGINT;\n", getDefinition().getGeneratedKeyColumn());
      }
      String alias = "S";
      ps.println("DECLARE v_TARGET_ROW_PRESENT BIGINT;");
      ps.println("DECLARE SQLCODE INT DEFAULT 0;");
      ps.format("DECLARE %s CURSOR FOR SELECT %s.LINE_NO,", CUR, alias);
      ps.println(getLoadSelectColumnsAliased(alias));
      if (getDefinition().getUpdateMode().equals(LoadDefinition.MIXED_MODE)) {
        if (getDefinition().getAllKeys().length == 0) {
          throw new IllegalArgumentException("Unable to generate mixed-mode stored procedure, no keys defined");
        }
        ps.println(", case when target." + getDefinition().getAllKeyName()[0] + " is null then 0 else 1 end as TARGET_ROW_PRESENT");
      } else if (getDefinition().getUpdateMode().equals(LoadDefinition.INSERT_MODE)) {
        ps.println(", 0 as TARGET_ROW_PRESENT");
      } else { // UPDATE_MODE
        ps.println(", 1 as TARGET_ROW_PRESENT");
      }
      ps.format("from %s %s\n", getLoadBufferTableName(), alias);
      ps.print(getLoadFromClauseLookupEntries(alias));
      if (getDefinition().getUpdateMode().equals(LoadDefinition.MIXED_MODE)) {
        ps.format("left outer join %s target on %s\n", getDefinition().getDestTable(), getAliasedKeyJoin("target", alias));
      }
      ps.format("where %s.LINE_NO >= v_CURRENT_LINE and %s.LINE_NO <= (v_CURRENT_LINE + v_COMMIT_SIZE) and %s.LINE_NO <= v_LAST_LINE;\n", alias, alias, alias);
      if (isIterativeMode()) {
        ps.format("SET v_LAST_LINE = p_END_LINE;\n");
        ps.format("SET v_CURRENT_LINE = p_START_LINE;\n");
      } else {
        ps.format("SET v_LAST_LINE = (Select MAX(LINE_NO) from %s);\n", getLoadBufferTableName());
      }
      ps.println("WHILE (v_CURRENT_LINE <= v_LAST_LINE) DO");
      ps.format("OPEN %s;\n", CUR);
      String intoList = getCursorFetchIntoList();
      ps.format("FETCH FROM %s INTO %s;\n", CUR, intoList);
      ps.println("WHILE (SQLCODE = 0) DO");
      if (getDefinition().getUpdateMode().equals(LoadDefinition.MIXED_MODE)) {
        ps.println("CASE v_TARGET_ROW_PRESENT");
        ps.println("WHEN 1 THEN");
      }
      if (!getDefinition().getUpdateMode().equals(LoadDefinition.INSERT_MODE)) {
        ps.format(" update %s set %s %s;\n", getDefinition().getDestTable(), getDestinationVariableUpdateAssignments(), getUpdateWhereClause());
        ps.format(" update %s set LOADED = 'U' where LINE_NO = v_LINE_NO;\n", getLoadBufferTableName());
        if (getDefinition().getPostUpdateInsertQueries() != null) {
          String[] statements = getDefinition().getPostUpdateInsertQueries();
          for (int i = 0; i < statements.length; i++) {
            ps.println(getVariableInsertVersionOfSql(statements[i]));
          }
        }
        if (getDefinition().getPostUpdateUpdateQueries() != null) {
          String[] statements = getDefinition().getPostUpdateUpdateQueries();
          for (int i = 0; i < statements.length; i++) {
            ps.println(getVariableInsertVersionOfSql(statements[i]));
          }
        }
      }
      if (getDefinition().getUpdateMode().equals(LoadDefinition.MIXED_MODE)) {
        ps.println("ELSE");
      }
      if (!getDefinition().getUpdateMode().equals(LoadDefinition.UPDATE_MODE)) {
        ps.format(" update %s set LOADED = 'I' where LINE_NO = v_LINE_NO;\n", getLoadBufferTableName());
        String insertColumnsPrefix = "";
        String insertValuesPrefix = "";
        if (getDefinition().isGenerateKey()) {
          ps.format(" SET v_%s = nextval for %s;\n", getDefinition().getGeneratedKeyColumn(), getDefinition().getGenerateKeySeq());
          if (!getDefinition().isGeneratedKeyFieldAlsoDefined()) {
            insertValuesPrefix = "v_" + getDefinition().getGeneratedKeyColumn() + ", ";
            insertColumnsPrefix = getDefinition().getGeneratedKeyColumn() + ", ";
          }
        }
        ps.format(" insert into %s (%s%s)\n      values (%s%s);\n", getDefinition().getDestTable(), insertColumnsPrefix, 
            getDestinationInsertColumnList(), insertValuesPrefix, getInsertVariableList());
        if (getDefinition().getPostInsertInsertQueries() != null) {
          String[] statements = getDefinition().getPostInsertInsertQueries();
          for (int i = 0; i < statements.length; i++) {
            ps.println(getVariableInsertVersionOfSql(statements[i]));
          }
        }
        if (getDefinition().getPostInsertUpdateQueries() != null) {
          String[] statements = getDefinition().getPostInsertUpdateQueries();
          for (int i = 0; i < statements.length; i++) {
            ps.println(getVariableInsertVersionOfSql(statements[i]));
          }
        }
      }
      if (getDefinition().getUpdateMode().equals(LoadDefinition.MIXED_MODE)) {
        ps.println("END CASE;");
      }
      ps.format("FETCH FROM %s INTO %s;\n", CUR, intoList);
      ps.println("END WHILE;");
      ps.format("CLOSE %s;\n", CUR);
      ps.println("COMMIT;");
      ps.println("SET v_CURRENT_LINE = v_CURRENT_LINE + v_COMMIT_SIZE + 1;"); 
      ps.println("END WHILE;");
      ps.println("END");
      //ps.println("@");
      ps.close();
    } catch (Throwable t) {
      t.printStackTrace();
      return "";
    }
    return new String(out.toByteArray(), UTF8);
  }

  private String getUpdateWhereClause() {
    if (getDefinition().getUpdateWhereClause() == null) {
      return "where " + getDestinationToVariableKeyJoin();
    } else {
      String w = getDefinition().getUpdateWhereClause();
      String[] keys = getDefinition().getAllKeyName();
      for (int i = 0; i < keys.length; i++) {
        w = w.replaceFirst("[?]", "v_" + keys[i]);
      }
      return w;
    }
  }

  protected String getStoredProcedureName() {
    return getLoadBufferTableName() + "_SP";
  }

  private String getVariableInsertVersionOfSql(String ibatisSyntaxStatement) {
    InlineParameterMapParser parser = new InlineParameterMapParser();
    TypeHandlerFactory typeHandlerFactory = new TypeHandlerFactory();
    SqlText sqlText = parser.parseInlineParameterMap(typeHandlerFactory, ibatisSyntaxStatement);
    ParameterMapping[] parameterMappings = sqlText.getParameterMappings();
    String parameterizedSql = sqlText.getText();
    for (int i = 0; i < parameterMappings.length; i++) {
      ParameterMapping mapping = parameterMappings[i];
      String variableName = "v_" + mapping.getPropertyName();
      parameterizedSql = parameterizedSql.replaceFirst("\\?", variableName);
    }
    return " " + parameterizedSql.trim() + ";";
  }

  private List<String> getNonExternal() {
    List<FieldDefinition> f = new Filter<FieldDefinition>(){
      @Override
      protected boolean filter(FieldDefinition o) {
        return !o.isExternal();
      }}.filterItems(getDefinition().getAllFields());
    return new MapCar<FieldDefinition, String>() {
      @Override
      public String map(FieldDefinition o) {
        return o.getFieldName();
      }
    }.run(f);
  }

  private String getDestinationInsertColumnList() {
    return Strings.getListString(getNonExternal(), ", ");
  }
  private String getInsertVariableList() {
    List<String> nonExternal = getNonExternal();
    nonExternal = new MapCar<String, String>() {
      @Override
      public String map(String o) {
        return "v_" + o;
      }
    }.run(nonExternal);
    return Strings.getListString(nonExternal, ", ");
  }

  private String getDestinationVariableUpdateAssignments() {
    List<String> fields = getNonExternal();
    List<String> resultColumns = new ArrayList<String>();
    for (String d : fields) {
      resultColumns.add(d + " = v_" + d);
    }
    return Strings.getListString(resultColumns, ", ");
  }

  private String getCursorFetchIntoList() {
    List<FieldDefinition> fields = getDefinition().getAllFields();
    List<String> resultColumns = new ArrayList<String>();
    resultColumns.add("v_LINE_NO");
    for (FieldDefinition d : fields) {
      resultColumns.add("v_" + d.getFieldName());
    }
    resultColumns.add("v_TARGET_ROW_PRESENT");
    return Strings.getListString(resultColumns, ", ");
  }

  private String getAliasedKeyJoin(String targetTableAlias, String sourceTableAlias) {
    List<String> joins = new ArrayList<String>();
    FieldDefinition[] keys = getDefinition().getAllKeys();
    for (int i = 0; i < keys.length; i++) {
      String sourceSide = sourceTableAlias + "." + keys[i];
      if (isDeferLookupResolution(keys[i])) {
        sourceSide = getDefferredLookupTableAlias(keys[i]) + "." + getLookupTableResultColumnName(keys[i]);
      }
      
      joins.add(targetTableAlias + "." + keys[i] + " = " + sourceSide);
    }
    return Strings.getListString(joins, "\n        and ");
  }

  private String getDefferredLookupTableAlias(FieldDefinition d) {
    int index = 0;
    List<FieldDefinition> allFields = getDefinition().getAllFields();
    for (FieldDefinition def : allFields) {
      if (isDeferLookupResolution(def)) {
        index++;
        if (def.equals(d)) {
          return "L" + index;
        }
      }
    }
    throw new RuntimeException("Could not find lookup from " + d);
  }

  private Object getDestinationToVariableKeyJoin() {
    // This join uses only the target table and the variables declared
    // that the main cursor is fetched into
    List<String> joins = new ArrayList<String>();
    String[] keys = getDefinition().getAllKeyName();
    for (int i = 0; i < keys.length; i++) {
      joins.add(keys[i] + " = " + "v_" + keys[i]);
    }
    return Strings.getListString(joins, "\n        and ");
  }
  private String getLoadSelectColumnsAliased(String alias) {
    List<FieldDefinition> fields = getDefinition().getAllFields();
    List<String> resultColumns = new ArrayList<String>();
    int uncachedLookupCount = 0;
    for (FieldDefinition d : fields) {
      if (isDeferLookupResolution(d)) {
        uncachedLookupCount++;
        String lookupTableColumn = getLookupTableResultColumnName(d);
        resultColumns.add("L" + uncachedLookupCount + "." + lookupTableColumn + " as " + d.getFieldName());
      } else {
        resultColumns.add(alias + "." + d.getFieldName());
      }
    }
    return Strings.getListString(resultColumns, ", ");
  }
  private String getLoadFromClauseLookupEntries(String alias) {
    List<FieldDefinition> fields = getDefinition().getAllFields();
    List<String> resultColumns = new ArrayList<String>();
    for (FieldDefinition d : fields) {
      if (isDeferLookupResolution(d)) {
        String lookupTableName = getLookupTableResultTableName(d);
        String joinType = "left outer join ";
        if (d.isKey() || !d.isOptional()) {
          joinType = "inner join ";
        }
        String lookupAlias = getDefferredLookupTableAlias(d);
        String lookupMatchJoin = getLookupTableQueryConditionJoin(d, alias, lookupAlias);
        resultColumns.add(joinType + lookupTableName + " " + lookupAlias + lookupMatchJoin + "\n");
      }
    }
    return Strings.getListString(resultColumns, " ");
  }


  private String getLookupTableQueryConditionJoin(FieldDefinition d, String mainTableAlias, String lookupAlias) {
    String query = d.getLookupDefinition().getLookupQuery();
    List<String> list = new ArrayList<String>();
    try {
      String[] params = d.getLookupDefinition().getLookupQueryParams();
      for (int i = 0; i < params.length; i++) {
        Pattern lhsMatcher = regex(".+\\W(\\w+)\\W*=\\W*#" + params[i] + "[:#].*");
        Matcher matcher = lhsMatcher.matcher(query);
        if (!matcher.matches()) {
          Pattern rhsMatcher = regex(".+#" + params[i] + "[:#]\\W*=\\W*(\\w+)\\W*=\\W*.*");
          matcher = rhsMatcher.matcher(query);
        }
        if (!matcher.matches()) {
          throw new RuntimeException("Unable to parse lookup query for " + d.getFieldName() + ": " + query);
        }
        String columnName = matcher.group(1);
        if (getDefinition().getField(params[i]).isLookupField() && !params[i].equals(d.getFieldName())) {
          FieldDefinition paramField = getDefinition().getField(params[i]);
          String lookupTableResultColumnName = getLookupTableResultColumnName(paramField);
          list.add(getDefferredLookupTableAlias(paramField) + "." + lookupTableResultColumnName + " = " + lookupAlias + "." + columnName);
        } else {
          list.add(mainTableAlias + "." + params[i] + " = " + lookupAlias + "." + columnName);
        }
      }
      return " on " + Strings.getListString(list, " and ");
    } catch (InvalidLookupQueryException e) {
      throw new RuntimeException(e);
    }
  }

  private String getLookupTableResultColumnName(FieldDefinition d) {
    String query = d.getLookupDefinition().getLookupQuery();
    Pattern pattern = regex(".*select\\s+(\\w+).+from.+");
    Matcher matcher = pattern.matcher(query);
    if (!matcher.matches()) {
      throw new RuntimeException("Unable to parse lookup query for " + d.getFieldName() + ": " + query);
    }
    String columnName = matcher.group(1);
    if (columnName.contains(".")) {
      columnName = columnName.substring(columnName.indexOf('.') + 1);
    }
    return columnName;
  }
  private String getLookupTableResultTableName(FieldDefinition d) {
    String query = d.getLookupDefinition().getLookupQuery();
    Pattern pattern = regex(".+from\\W(\\w+).+");
    Matcher matcher = pattern.matcher(query);
    if (!matcher.matches()) {
      throw new RuntimeException("Unable to parse lookup query for " + d.getFieldName());
    }
    String tableName = matcher.group(1);
    return tableName;
  }

  private Pattern regex(String p) {
    Pattern pattern = Pattern.compile(p, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE| Pattern.DOTALL);
    return pattern;
  }

  private List<String> getFileLocations(List<LoadLocation> locations) {
    return new MapCar<LoadLocation, String>() {
      @Override
      public String map(LoadLocation ll) {
        return ll.start + " " + ll.end;
      }
    }.run(locations);
  }

  private List<Integer> getNullIndicatorLocations(List<LoadLocation> locations) {
    return new MapCar<LoadLocation, Integer>() {
      @Override
      public Integer map(LoadLocation ll) {
        return ll.nullInd;
      }
    }.run(locations);
  }

  @Override
  public void complete(File sourceFile, SqlMapClient writerClient, LoadingErrorList l) {
    try {
      fw.close();
    } catch (Exception e) {
      // Ignore this
    }
  }
  protected boolean isIterativeMode() {
    return isIterativeMode;
  }
  protected int getMaxLineWritten() {
    return maxLine;
  }

  public void setOutputFileDirectory(String fileOutputPath) {
    this.fileOutputPath = fileOutputPath;
  }
  protected String getFileOutputPath() {
    return fileOutputPath;
  }
}
