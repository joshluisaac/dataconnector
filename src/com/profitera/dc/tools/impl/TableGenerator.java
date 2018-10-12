package com.profitera.dc.tools.impl;

import java.util.ArrayList;
import java.util.List;

import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.impl.FieldTypeInfoRegistry;
import com.profitera.dc.impl.IFieldTypeInfo;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.util.Strings;
import com.profitera.util.reflect.ReflectionException;

public class TableGenerator {
  private final LoadDefinition def;

  public TableGenerator(LoadDefinition definition) {
    this.def = definition;
  }

  public String getTargetTable() throws ReflectionException {
    return renderTableDefinition(false);
  }

  protected String renderTableDefinition(boolean isExternal) throws ReflectionException {
    FieldTypeInfoRegistry types = new FieldTypeInfoRegistry(def);
    List<FieldDefinition> allFields = def.getAllFields();
    String tableName = def.getDestTable();
    if (isExternal) {
      tableName = getExternalTableName();
    }
    StringBuilder b = new StringBuilder("CREATE TABLE " + tableName + " (\n");
    if (def.isGenerateKey()) {
      b.append(" " + def.getGeneratedKeyColumn() + " " + "BIGINT NOT NULL PRIMARY KEY,\n");
    }
    List<String> columnDefinitions = new ArrayList<String>();
    for (FieldDefinition d : allFields) {
      if (d.isExternal() != isExternal) {
        continue;
      }
      IFieldTextHandler handler = types.getHandler(d);
      IFieldTypeInfo info = types.getHandlerTypeInfo(d);
      String databaseColumnDefinition = info.getDatabaseColumnDefinition(d, info.getFieldTextWidth(d, handler));
      String notNull = " NOT NULL";
      if (d.isOptional()) {
        notNull = "";
      }
      columnDefinitions.add(" " + databaseColumnDefinition + notNull);
    }
    b.append(Strings.getListString(columnDefinitions, ",\n"));
    b.append("\n)");
    return b.toString();
  }

  private String getExternalTableName() {
    return def.getDestTable() + "_EXT";
  }

  public String getTableForExternalFields() throws ReflectionException {
    return renderTableDefinition(true);
  }

  public String getInsertForExternalTable() {
    List<FieldDefinition> allFields = def.getAllFields();
    StringBuilder more = new StringBuilder(";\n\n");
    List<String> columns = new ArrayList<String>();
    for (FieldDefinition d : allFields) {
      if (!d.isExternal()) {
        continue;
      }
      columns.add(d.getFieldName());
    }
    more.append("INSERT INTO " + def.getDestTable() + "_EXT (");
    more.append(Strings.getListString(columns, ", "));
    more.append(")\nvalues\n(#");
    more.append(Strings.getListString(columns, "#, #"));
    more.append("#)");
    return more.toString();
  }
  public String getUpdateForExternalTable() {
    List<FieldDefinition> allFields = def.getAllFields();
    StringBuilder more = new StringBuilder(";\n\n");
    List<String> columns = new ArrayList<String>();
    for (FieldDefinition d : allFields) {
      if (!d.isExternal()) {
        continue;
      }
      columns.add(d.getFieldName() + " = #" + d.getFieldName() + "#");
    }
    more.append("UPDATE " + getExternalTableName() + " set ");
    more.append(Strings.getListString(columns, ",\n "));
    more.append("\nwhere ...");
    return more.toString();
  }
}
