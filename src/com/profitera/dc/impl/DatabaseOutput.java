package com.profitera.dc.impl;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.profitera.dc.ErrorSummary;
import com.profitera.dc.ILoadingProcess;
import com.profitera.dc.LoadingRowInsertException;
import com.profitera.dc.LoadingRowUpdateException;
import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.parser.LoadingQueryWriter;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.ibatis.SQLMapTransactor;

public class DatabaseOutput extends AbstractLoaderOutput implements ILoaderOutput {
  private final boolean isBatch;
  public DatabaseOutput(String name, LoadDefinition d, boolean isBatch) {
    super(name, d);
    this.isBatch = isBatch;
  }
  @Override
  public void generateOutput(final SqlMapClient writer, final List<String> linesRead, long startLineNo,
      final Map<String, IFieldTextHandler> handlers, ErrorSummary errorSummary, final File sourceFile,
      final List<String> badLines, final List<Exception> exceptions, final List<String> statementNames,
      final List<Map<String, Object>> statementArgs) throws SQLException {
    long commitsFailed = 0;
    long dbRequestFailed = errorSummary.getDbRequestTooLong();
    SQLMapTransactor sqlmt = new SQLMapTransactor(writer, (int)startLineNo, (int)startLineNo + linesRead.size() - 1) {
      public int transaction(SqlMapClient client) throws SQLException {
        int dbError = 0;
        try {
          if (isBatch()) {
            client.startBatch();
          }
          executeUpdates(client, statementNames, statementArgs, startLineNo, sourceFile, handlers);
          if (isBatch()) {
            client.executeBatch();
          }
        } catch (Exception ex) {
          logLineError(super.startLineNo, super.endLineNo, "Failed inserting/updating data", ex);
          exceptions.add(new Exception("Line " + (startLineNo + linesRead.size() - 1), ex));
          // The lines affected by the failure is only those that would have
          // been used here
          // that means subtracting the lines that were already in the bad list.
          dbError = linesRead.size() - badLines.size();
          // Now everything is bad, to maintain the order of lines in the bad
          // file
          // just clear it and add everything.
          badLines.clear();
          badLines.addAll(linesRead);
          throw new RuntimeException("COMMIT", ex);
        }
        return dbError;
      }
    };
    
    if (((ILoadingProcess) Thread.currentThread()).isValid()) {
      int unexpectedError = 0;
      try {
        // Inform the executing thread that there is a db call being made
        ((ILoadingProcess) Thread.currentThread()).startDbCall("BULK COMMIT");
        unexpectedError = sqlmt.execute();
      } catch (RuntimeException e) {
        if ("COMMIT".equals(e.getMessage())) {
          unexpectedError = linesRead.size();
        } else {
          throw e;
        }
      } finally {
        // Inform the executing thread that the db call has been completed
        ((ILoadingProcess) Thread.currentThread()).endDbCall();
        if (!((ILoadingProcess) Thread.currentThread()).isValid()) {
          dbRequestFailed = unexpectedError;
        } else {
          commitsFailed = unexpectedError;
        }
      }
    }
    errorSummary.setCommitFailed(commitsFailed);
    errorSummary.setDbRequestTooLong(dbRequestFailed);
    
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
  private void executeUpdates(final SqlMapClient client, final List<String> statementNames, final List<Map<String, Object>> statementArgs, int startLine, File sourceFile, Map<String, IFieldTextHandler> handlers) {
    SqlMapClient writer = client;
    for (int i = 0; i < statementNames.size(); i++) {
      String name = statementNames.get(i);
      Map<String, Object> args = statementArgs.get(i);
      executeUpdates(writer, name, args, startLine + i, sourceFile, handlers);
    }
  }
  public void executeUpdates(final SqlMapClient client, final String statementName, final Map<String, Object> statementArgs,
      int line, File sourceFile, Map<String, IFieldTextHandler> handlers) {
    executeUpdatesForDB(client, statementName, statementArgs);
  }


  protected void executeUpdatesForDB(final SqlMapClient client, final String statementName, final Map<String, Object> statementArgs) {
    String destTable = getDefinition().getDestTable();
    SqlMapClient writer = client;
    if (statementName.equals(LoadingQueryWriter.getInsertName(destTable))) {
      Object key = null;
      try {
        key = writer.insert(statementName, statementArgs);
      } catch (SQLException e) {
        throw new LoadingRowInsertException("Failed to execute " + LoadingQueryWriter.getInsertName(destTable) + ":"
            + statementArgs, e);
      }
      getLog().emit(DataLoaderLogClient.LOAD_DB_INSERT, getLoadName(), statementArgs);
      if (getDefinition().isGenerateKey()) {
        String keyField = getDefinition().getGeneratedKeyColumn();
        statementArgs.put(keyField, key);
        getLog().emit(DataLoaderLogClient.LOAD_DB_INSERT_KEY, getLoadName(), key);
      }
      if (getDefinition().getPostInsertUpdateQueries() != null) {
        for (int i = 0; i < getDefinition().getPostInsertUpdateQueries().length; i++) {
          try {
            writer.update(LoadingQueryWriter.getPostInsertUpdateName(destTable, i), statementArgs);
          } catch (SQLException e) {
            throw new LoadingRowInsertException("Failed to execute post-insert "
                + LoadingQueryWriter.getPostInsertUpdateName(destTable, i) + ":" + statementArgs, e);
          }
        }
        getLog().emit(DataLoaderLogClient.LOAD_DB_POST_INSERT_UPDATE, getLoadName(), statementArgs);
      }
      if (getDefinition().getPostInsertInsertQueries() != null) {
        for (int i = 0; i < getDefinition().getPostInsertInsertQueries().length; i++) {
          try {
            writer.insert(LoadingQueryWriter.getPostInsertInsertName(destTable, i), statementArgs);
          } catch (SQLException e) {
            throw new LoadingRowInsertException("Failed to execute post-insert "
                + LoadingQueryWriter.getPostInsertInsertName(destTable, i) + ":" + statementArgs, e);
          }
        }
        getLog().emit(DataLoaderLogClient.LOAD_DB_POST_INSERT_INSERT, getLoadName(), statementArgs);
      }
    } else {
      try {
        writer.update(statementName, statementArgs);
      } catch (SQLException e) {
        throw new LoadingRowUpdateException("Failed to execute " + LoadingQueryWriter.getUpdateName(destTable) + ":"
            + statementArgs, e);
      }
      getLog().emit(DataLoaderLogClient.LOAD_DB_UPDATE, getLoadName(), statementArgs);
      if (getDefinition().getPostUpdateUpdateQueries() != null) {
        for (int i = 0; i < getDefinition().getPostUpdateUpdateQueries().length; i++) {
          try {
            writer.update(LoadingQueryWriter.getPostUpdateUpdateName(destTable, i), statementArgs);
          } catch (SQLException e) {
            throw new LoadingRowInsertException("Failed to execute post-update "
                + LoadingQueryWriter.getPostUpdateUpdateName(destTable, i) + ":" + statementArgs, e);
          }
        }
        getLog().emit(DataLoaderLogClient.LOAD_DB_POST_UPDATE_UPDATE, getLoadName(), statementArgs);
      }
      if (getDefinition().getPostUpdateInsertQueries() != null) {
        for (int i = 0; i < getDefinition().getPostUpdateInsertQueries().length; i++) {
          try {
            writer.insert(LoadingQueryWriter.getPostUpdateInsertName(destTable, i), statementArgs);
          } catch (SQLException e) {
            throw new LoadingRowInsertException("Failed to execute post-update "
              + LoadingQueryWriter.getPostUpdateInsertName(destTable, i) + ":" + statementArgs, e);
          }
        }
        getLog().emit(DataLoaderLogClient.LOAD_DB_POST_UPDATE_INSERT, getLoadName(), statementArgs);
      }
    }
  }
  private void logLineError(long startLineNumber, long endLineNumber, String message, Throwable e) {
    getLog().emit(DataLoaderLogClient.LOAD_DB_LINE_ERROR, e, startLineNumber, endLineNumber, message);
  }
  private boolean isBatch() {
    return isBatch;
  }
}
