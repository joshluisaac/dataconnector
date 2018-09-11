package com.profitera.dc.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.profitera.dc.LoadingErrorList;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.ibatis.SQLMapFileRenderer;
import com.profitera.util.Holder;
import com.profitera.util.Strings;
import com.profitera.util.io.FileUtil;
import com.profitera.util.io.ForEachLine;

public class NativeDatabaseLoaderOutput extends FixedWidthFileOutput {
  private static final String CREATE_T = "createNativeLoaderIntermediateTable";
  private static final String CREATE_T_INDEX = "createNativeLoaderIntermediateTableIndex";
  private static final String DROP_T = "dropNativeLoaderIntermediateTable";
  private static final String CREATE_SP = "createNativeLoaderStoredProcedure";
  private static final String DROP_SP = "dropNativeLoaderStoredProcedure";
  private static final String RUN_LOAD = "runNativeLoaderLoad";
  private static final String RUN_SP = "runNativeLoaderStoredProcedure";
  private static final String SELECT_NOT_LOADED = "selectUnloadedNativeLoaderLineNumbers";
  private final String copyCommand;
  private final String serverSourceFileDirectory;
  private final String logDirectory;
  private final boolean isRebuilding;
  private final Charset sourceEncoding;

  public NativeDatabaseLoaderOutput(String name, LoadDefinition d, boolean isRebuilding, String copyCommand, String serverFileDir, String logDirectory, Charset sourceEncoding) {
    super(name, d);
    this.isRebuilding = isRebuilding;
    this.copyCommand = copyCommand;
    this.sourceEncoding = sourceEncoding;
    this.serverSourceFileDirectory = Strings.coalesce(serverFileDir, "");
    this.logDirectory = logDirectory;
  }

  @Override
  public void writeOutputSpecificSql(BufferedWriter xmlBuffer) throws IOException {
    super.writeOutputSpecificSql(xmlBuffer);
    SQLMapFileRenderer r = new SQLMapFileRenderer();
    xmlBuffer.write(r.renderUpdate(CREATE_T, Map.class, getIntermediateTableDefinition()));
    xmlBuffer.write(r.renderUpdate(CREATE_T_INDEX, Map.class, "CREATE INDEX IX" + getLoadBufferTableName() + " ON " + getLoadBufferTableName() + "(LINE_NO, LOADED) ALLOW REVERSE SCANS"));
    xmlBuffer.write(r.renderUpdate(DROP_T, Map.class, "Drop table " + getLoadBufferTableName()));
    xmlBuffer.write(r.renderUpdate(CREATE_SP, Map.class, getStoredProcedure()));
    xmlBuffer.write(r.renderUpdate(DROP_SP, Map.class, "Drop procedure " + getStoredProcedureName()));
    xmlBuffer.write(r.renderUpdate(RUN_LOAD, Map.class, getNativeLoadCommand(true)));
    if (isIterativeMode()) {
      xmlBuffer.write(r.renderUpdate(RUN_SP, Map.class, "call " + getStoredProcedureName() + "(#START_LINE#, #END_LINE#)"));
    } else {
      xmlBuffer.write(r.renderUpdate(RUN_SP, Map.class, "call " + getStoredProcedureName() + "()"));
    }
    xmlBuffer.write(r.renderSelect(SELECT_NOT_LOADED, Long.class, "select LINE_NO from "
      + getLoadBufferTableName() + " where LOADED = 'N' order by LINE_NO"));
  }

  @Override
  public void complete(File sourceFile, SqlMapClient writerClient, LoadingErrorList errors) {
    super.complete(sourceFile, writerClient, errors);
    if (getOutputFile() == null) {
      return; //Nothing to load, not file generated, all records must have been rejected.
    }
    String outputFullPath = FileUtil.tryCanonical(getOutputFile());
    if (copyCommand != null) {
      try {
        final Process exec = Runtime.getRuntime().exec(
            copyCommand + " \"" + outputFullPath + "\" \"" + getOutputFile().getName() + "\"");
        readAllOfStreamAsynchronously(copyCommand, exec.getInputStream());
        readAllOfStreamAsynchronously(copyCommand, exec.getErrorStream());
        int exitValue = exec.waitFor();
        if (exitValue != 0) {
          throw new RuntimeException("Copy command for native loading of " + getLoadName() + " returned exit code of "
              + exitValue);
        }
      } catch (IOException e) {
        errors.setNativeLoaderFailure(true);
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        errors.setNativeLoaderFailure(true);
        // I suspect this will never happen
        throw new RuntimeException(e);
      }
    }
    if (isRebuilding) {
      getLog().emit(DataLoaderLogClient.LOAD_RECREATE_NATIVE, getLoadName());
      updateIgnoreFail(DROP_T, writerClient);
      updateIgnoreFail(DROP_SP, writerClient);
    } else {
      getLog().emit(DataLoaderLogClient.LOAD_NOT_RECREATE_NATIVE, getLoadName());
    }
    try {
      if (isRebuilding) {
        update(CREATE_T, null, writerClient);
        update(CREATE_T_INDEX, null, writerClient);
        update(CREATE_SP, null, writerClient);
      }
      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put("FILE", "''" + serverSourceFileDirectory + getOutputFile().getName() + "''");
      getLog().emit(DataLoaderLogClient.RUN_NATIVE_LOAD, getLoadName());
      update(RUN_LOAD, params, writerClient);
      int max = getMaxLineWritten();
      int start = 1;
      int threadCount = getThreads();
      // If there are very few lines to load, just skip
      // the concurrency altogether.
      if (threadCount >= 2*getMaxLineWritten()) {
        threadCount = 1;
      }
      int perThread = max / threadCount;
      Thread[] threads = new Thread[threadCount];
      List<Holder<SQLException>> exceptions = new ArrayList<Holder<SQLException>>();
      for (int i = 0; i < threadCount; i++) {
        int end = start + perThread - 1;
        if (i == threadCount - 1) {
          end = getMaxLineWritten();
        }
        exceptions.add(new Holder<SQLException>());
        threads[i] = getStoredProcedureThreadForLines(start, end, writerClient, exceptions.get(i));
        start = end + 1;
      }
      for (int i = 0; i < threads.length; i++) {
        threads[i].start();
      }
      for (int i = 0; i < threads.length; i++) {
        try {
          threads[i].join();
        } catch (InterruptedException e) {
          // Ignore this, it should not happen
        }
      }
      for (Holder<SQLException> holder : exceptions) {
        if (holder.get() != null) {
          throw holder.get();
        }
      }
    } catch (SQLException e) {
      errors.setNativeLoaderFailure(true);
      throw new RuntimeException(e);
    }
    try {
      // If we can't write to the log directory the loader itself has already logged that fact
      if (logDirectory != null && new File(logDirectory).isDirectory()) {
        final List<Long> unloadedLines = getUnloadedLinesList(writerClient);
        int notLoadedCount = unloadedLines.size();
        final Holder<Long> currentFileLine = new Holder<Long>(0L);
        final PrintStream nativeBad = getFileManager().getPrintWriter(new File(logDirectory, getLoadName()+"-nativeloader.bad"), sourceEncoding);
        final BufferedReader bufferedReader = new BufferedReader(new FileReader(sourceFile));
        ForEachLine l = new ForEachLine(bufferedReader) {
          @Override
          protected void process(String line) {
            currentFileLine.set(currentFileLine.get() + 1);
            if (unloadedLines.size() == 0) {
              return;
            }
            if (currentFileLine.get().equals(unloadedLines.get(0))) {
              nativeBad.println(line);
              unloadedLines.remove(0);
            }
          }
        };
        l.process();
        bufferedReader.close();
        nativeBad.close();
        errors.setNativeLoaderNotLoaded(notLoadedCount);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Thread getStoredProcedureThreadForLines(final int start, final int end, final SqlMapClient writerClient, final Holder<SQLException> h) {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        Map<String, Object> spParams = new HashMap<String, Object>();
        spParams.put("START_LINE", start);
        spParams.put("END_LINE", end);
        getLog().emit(DataLoaderLogClient.RUN_SP, getLoadName(), start, end);
        long startTime = System.currentTimeMillis();
        try {
          update(RUN_SP, spParams, writerClient);
        } catch (SQLException e) {
          h.set(e);
        }
        long elapsed = System.currentTimeMillis() - startTime;
        long elapsedSeconds = 1;
        if (elapsed/1000 > 1) {
          elapsedSeconds = elapsed / 1000;
        }
        getLog().emit(DataLoaderLogClient.END_SP, getLoadName(), start, end, (end - start + 1)/elapsedSeconds);
      }
    });
  }

  @SuppressWarnings("unchecked")
  private List<Long> getUnloadedLinesList(SqlMapClient writerClient) throws SQLException {
    return (List<Long>) writerClient.queryForList(SELECT_NOT_LOADED);
  }
  private void readAllOfStreamAsynchronously(final String command, final InputStream s) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          new ForEachLine(new BufferedReader(new InputStreamReader(s))){
            @Override
            protected void process(String line) {
              if (line.trim().length() > 0) {
                getLog().emit(DataLoaderLogClient.EXT_SCRIPT_OUT, command, line);
              }
            }}.process();
        } catch (IOException e) {
          getLog().emit(DataLoaderLogClient.EXT_SCRIPT_OUT, e, command, "");
        }
      }
    }).start();
  }

  private boolean updateIgnoreFail(String s, SqlMapClient c) {
    try {
      update(s, new HashMap<String, Object>(), c);
      return true;
    } catch (SQLException e) {
      // Ignored Intentionally
      return false;
    }
  }

  private void update(String s, Map<String, Object> params, SqlMapClient c) throws SQLException {
    try {
      c.startTransaction();
      c.update(s, params);
      c.commitTransaction();
    } finally {
      c.endTransaction();
    }
  }
}
