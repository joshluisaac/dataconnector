package com.profitera.dc.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.profitera.dc.LoadingErrorList;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.log.DefaultLogProvider;
import com.profitera.log.ILogProvider;

public abstract class AbstractLoaderOutput implements ILoaderOutput {
  private final LoadDefinition definition;
  private final String loadName;
  private FileHandleManager fileManager;
  private DefaultLogProvider l;
  private int commitSize = 100;
  private int threads = 5;

  public AbstractLoaderOutput(String loadName, LoadDefinition d) {
    this.loadName = loadName;
    this.definition = d;
    l = new DefaultLogProvider();
    l.register(new DataLoaderLogClient());
  }
  public void setCommitSize(int s) {
    this.commitSize = s;
  }

  protected int getCommitSize() {
    return commitSize;
  }
  public void setThreadCount(int s) {
    this.threads = s;
  }

  protected int getThreads() {
    return threads;
  }

  protected LoadDefinition getDefinition() {
    return definition;
  }
  protected String getLoadName() {
    return loadName;
  }
  public void setFileHandleManager(FileHandleManager m) {
    this.fileManager = m;
  }
  protected FileHandleManager getFileManager() {
    if (fileManager == null) {
      throw new IllegalStateException("Attempt made to access file handle manager before assigned for output for " + getLoadName());
    }
    return fileManager;
  }
  @Override
  public boolean isDeferLookupResolution(FieldDefinition field) {
    return false;
  }
  @Override
  public void writeOutputSpecificSql(BufferedWriter xmlBuffer) throws IOException {
    // By default there is nothing extra to write
  }
  @Override
  public void complete(File sourceFile, SqlMapClient writerClient, LoadingErrorList e) {
    // By default there is nothing to do here
  }

  protected ILogProvider getLog() {
    return l;
  }

}
