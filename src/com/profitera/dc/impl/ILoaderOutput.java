package com.profitera.dc.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.profitera.dc.ErrorSummary;
import com.profitera.dc.LoadingErrorList;
import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.parser.impl.FieldDefinition;

public interface ILoaderOutput {
  void generateOutput(final SqlMapClient writer, final List<String> linesRead, long startLineNo,
      final Map<String, IFieldTextHandler> handlers, ErrorSummary errorSummary, final File sourceFile,
      final List<String> badLines, final List<Exception> exceptions, final List<String> statementNames,
      final List<Map<String, Object>> statementArgs) throws SQLException;
  void setFileHandleManager(FileHandleManager m);
  boolean isDeferLookupResolution(FieldDefinition field);
  void writeOutputSpecificSql(BufferedWriter xmlBuffer) throws IOException;
  void complete(File sourceFile, SqlMapClient writerClient, LoadingErrorList allErrors);
}
