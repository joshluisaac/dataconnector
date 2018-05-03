package com.profitera.dc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;

import com.ibatis.common.jdbc.SimpleDataSource;
import com.ibatis.sqlmap.client.SqlMapClient;
import com.ibatis.sqlmap.client.event.RowHandler;
import com.profitera.datasource.DataSourceUtil;
import com.profitera.datasource.IDataSourceConfiguration;
import com.profitera.datasource.IDataSourceConfigurationSet;
import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.handler.IFieldTextHandlerContext;
import com.profitera.dc.impl.ControlFileManager;
import com.profitera.dc.impl.ControlFileWaitExpiredException;
import com.profitera.dc.impl.DataLoaderLogClient;
import com.profitera.dc.impl.DatabaseOutput;
import com.profitera.dc.impl.DefaultFieldTextHandlerContext;
import com.profitera.dc.impl.DeltaGenerator;
import com.profitera.dc.impl.FieldParsingFormatException;
import com.profitera.dc.impl.FileHandleManager;
import com.profitera.dc.impl.FixedWidthFileOutput;
import com.profitera.dc.impl.ILoaderOutput;
import com.profitera.dc.impl.LoadFileKeyScanner;
import com.profitera.dc.impl.NativeDatabaseLoaderOutput;
import com.profitera.dc.impl.QueryLoadRowResolver;
import com.profitera.dc.impl.StaticLoadRowResolver;
import com.profitera.dc.impl.TableRefresher;
import com.profitera.dc.lookup.DefaultLookupCache;
import com.profitera.dc.lookup.ILookupCache;
import com.profitera.dc.lookup.ParameterCache;
import com.profitera.dc.lookup.impl.DataLoaderLookupLogClient;
import com.profitera.dc.parser.LoadConfigurationParser;
import com.profitera.dc.parser.LoadConfigurationParser.OutputType;
import com.profitera.dc.parser.LoadDefinitionValidator;
import com.profitera.dc.parser.LoadingQueryWriter;
import com.profitera.dc.parser.V2LoadDefinitionParser;
import com.profitera.dc.parser.exception.InvalidConfigurationException;
import com.profitera.dc.parser.exception.LoadNotExistException;
import com.profitera.dc.parser.exception.NotParsedException;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.GlobalFilter;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.dc.parser.impl.LookupDefinition;
import com.profitera.ibatis.SQLMapResource;
import com.profitera.log.DefaultLogProvider;
import com.profitera.log.ILogProvider;
import com.profitera.util.DateParser;
import com.profitera.util.Holder;
import com.profitera.util.IRecordDispenser;
import com.profitera.util.Strings;
import com.profitera.util.Utilities;
import com.profitera.util.io.FileUtil;
import com.profitera.util.reflect.ClasspathSearcher;
import com.profitera.util.reflect.Reflect;
import com.profitera.util.reflect.ReflectionException;
import com.profitera.util.xml.DocumentLoader;

public class DataLoader {
  private static final Charset UTF8 = Charset.forName("UTF8");
  private static ICommitListener DUMMY_COMMIT_LISTENER = new AbstractCommitListener() {};
  private enum RunState {NONE, RUNNING, COMPLETE};
  public static final String MAP_FILE_PROPERTY = "MAP_FILE";
  public static final String DB_ENCRYPTED_PASSWORD_PROPERTY = "PASSWORD";
  public static final int SERIAL_MODE = 1;
  public static final int BATCH_MODE = 2;
  private int mode = SERIAL_MODE;
  private static final Log LOG = LogFactory.getLog(DataLoader.class);
  private static ILogProvider logger;
  // Config fields:
  private int threadCount = 10;
  private int commitSize = 10;
  private String loadName = "UNKNOWN";
  private boolean shareReadAndWrite = false;
  public static final String CONN_CONFIG = "connConfig.xml";
  private static final int KEY_SCAN_BATCH_SIZE = 100000;
  //
  protected LoadDefinition definition = null;
  private ParameterCache rowLookupCache = null;
  private ICommitListener commitListener = DUMMY_COMMIT_LISTENER;

  private Map<String, ILookupCache> lookupCache = new HashMap<String, ILookupCache>();

  private boolean stopLoading = false;
  private RunState runState = RunState.NONE;
  private boolean printFilterRecordToFile = true;
  private ILoadRowResolver loadRowResolver;

  public DataLoader(int threads, int commits, String name) {
    threadCount = threads;
    commitSize = commits;
    setLoadName(name);
  }


  public void configureLoad(File configFile, String loadName) throws InvalidLookupQueryException, InvalidConfigurationException,
      IOException {
    String path = FileUtil.tryCanonical(configFile);
    getLog().emit(DataLoaderLogClient.WILL_PARSE_LOAD, path, loadName);
    Document doc = null;
    try {
      doc = DocumentLoader.loadDocument(configFile);
      configureLoad(doc, path);
    } catch (RuntimeException e) {
      if (doc == null) {
        getLog().emit(DataLoaderLogClient.NOT_XML_PARSE_LOAD, FileUtil.tryCanonical(configFile), loadName);
        Properties defBuffer = new Properties();
        defBuffer.load(new FileInputStream(configFile));
        configureLoad(defBuffer);
      } else {
        throw e;
      }
    }
  }

  public void configureLoad(Document configDoc, String path) throws InvalidConfigurationException, InvalidLookupQueryException {
    definition = V2LoadDefinitionParser.parse(configDoc, path);
    LoadDefinitionValidator.validate(definition);
  }

  @SuppressWarnings("deprecation")
  private void configureLoad(Properties configProp) throws InvalidConfigurationException, InvalidLookupQueryException {
    definition = V2LoadDefinitionParser.parse(configProp);
    LoadDefinitionValidator.validate(definition);
  }

  public void configureLoad(LoadDefinition definition) throws InvalidConfigurationException,
      InvalidLookupQueryException {
    this.definition = definition;
    LoadDefinitionValidator.validate(definition);
  }

  public LoadingErrorList loadFile(final String file, final IDataSourceConfiguration dataSource,
      final String logDir, final long timeout, ILoaderOutput out, final File previousFileForDiffing, final boolean isAllowingSortForDiff, final Charset sourceCharset)
      throws FileNotFoundException, IOException {
    final File source = new File(file);
    if (out == null) {
      out = new DatabaseOutput(getLoadName(), definition, isBatch());
    }
    final ILoaderOutput output = out;
    final Holder<LoadingErrorList> h = new Holder<LoadingErrorList>();
    new FileHandleManager() {
      @Override
      protected void with(FileHandleManager m) throws IOException {
        output.setFileHandleManager(m);
        File diagnosticDir = null;
        if (logDir != null) {
          diagnosticDir = new File(logDir);
        }
        h.set(loadFileInternal(source, dataSource, diagnosticDir, timeout, m, output, previousFileForDiffing, isAllowingSortForDiff, sourceCharset));
      }}.withManager();
    return h.get();
  }

  private LoadingErrorList loadFileInternal(final File sourceDataFile, IDataSourceConfiguration dataSource,
      File badDir, long monitorTimeout, final FileHandleManager m, final ILoaderOutput output, File previousSourceDataFile, boolean isAllowingSortForDiff, final Charset sourceCharset) throws IOException {
    if (!sourceDataFile.exists() || !sourceDataFile.canRead()) {
      throw new FileNotFoundException("Unable to read source file " + sourceDataFile
          + " (" + FileUtil.tryCanonical(sourceDataFile) + ") for process " + loadName);
    }
    final Properties connectionProps = getConnectionProperties(dataSource);
    BufferedWriter badFile = null;
    PrintStream stacktraceFile = null;
    BufferedWriter filterFile = null;
    if (badDir == null) {
      LOG.info("Diagnostic file directory is not provided, bad, stacktrace, and filter file writing disabled.");
    } else if (!badDir.exists() || !badDir.canWrite()) {
      LOG.info("Diagnostic file directory " + badDir.getAbsolutePath() + " does not exist or not writable, bad, stacktrace, and filter file writing disabled.");
    } else {
      badFile = m.getWriter(new File(badDir, getLoadName() + ".bad"));
      stacktraceFile = m.getPrintWriter(new File(badDir, getLoadName() + ".stacktrace"), UTF8);
      if (printFilterRecordToFile) {
        File filteredRecordsFile = new File(badDir, getLoadName() + ".filter");
        filterFile = m.getWriter(filteredRecordsFile);
      }
    }
    if (runState != RunState.NONE) {
      throw new IllegalStateException("Attempt made to rerun load that was already executed " + loadName);
    }
    runState = RunState.RUNNING;
    final LoadingErrorList allErrors = new LoadingErrorList(badFile, stacktraceFile, filterFile);
    if (definition == null) {
      throw new IllegalStateException("LoadDefinitionParser is not initialized");
    }
    URL xmlConfigFile = writeSqlXmlContent(m, output);
    connectionProps.put(MAP_FILE_PROPERTY, xmlConfigFile.toString());
    getLog().emit(DataLoaderLogClient.LOAD_GENERATED_CONF, xmlConfigFile.toString(), getLoadName());
    // check connection for the first time before starting
    final SqlMapClient writerClient = getWriterClient(CONN_CONFIG, connectionProps);
    final SqlMapClient readOnlyClient = getReadOnlyClient(CONN_CONFIG, connectionProps, writerClient);
    try {
      checkConnection(writerClient);
      checkConnection(readOnlyClient);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    getLog().emit(DataLoaderLogClient.LOAD_FROM, FileUtil.tryCanonical(sourceDataFile), getLoadName());
    String version = ClasspathSearcher.getManifestValueForJarList(this.getClass(), new String[]{"ptrdataconnector"}, "DataConnector-Version", "[Unknown]");
    getLog().emit(DataLoaderLogClient.LOAD_WITH, version, getLoadName());
    final long startTime = System.currentTimeMillis();
    String destTable = definition.getDestTable();
    doKeyVerification(sourceDataFile, sourceCharset);
    // This will be the place to generate a delta.
    DeltaGenerator delta = new DeltaGenerator(getLoadName(), getDefinition(), getLog(), sourceCharset);
    final File dataFile = delta.generateDeltaFile(sourceDataFile, previousSourceDataFile, m, isAllowingSortForDiff);
    final IRecordDispenser<String> dispenser = (IRecordDispenser<String>) buildDispenserForFile(dataFile, definition, commitSize, sourceCharset);
    new TableRefresher(definition).refresh(writerClient, allErrors);
    loadFullCache(destTable, readOnlyClient, allErrors);
    initLookupCaches(writerClient, readOnlyClient);
    DataLoaderExecutionThread[] threads = new DataLoaderExecutionThread[threadCount];
    Semaphore semaphore = new Semaphore(1, true);
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new DataLoaderExecutionThread(allErrors, semaphore) {
        public void run() {
          try {
            loadFromDispenser(startTime, dispenser, writerClient, readOnlyClient, connectionProps, allErrors, m, dataFile, output);
          } catch (IOException e) {
            LOG.fatal("I/O failure, one loading process terminated: " + Thread.currentThread().getName(), e);
          } catch (SQLException sqle) {
            LOG.fatal("Database related failure, please refer to error below. Aborting data loader.");
            LOG.fatal(sqle);
            throw new RuntimeException("Database connection failure.", sqle);
          } catch (Exception e) {
            LOG.fatal("Exception occured, one loading process terminated: " + Thread.currentThread().getName(), e);
          }
        }
      };
      threads[i].setName(getLoadName() + "-thread-" + (i + 1));
    }
    DataLoaderMonitorThread monitor = new DataLoaderMonitorThread(threads, monitorTimeout);
    monitor.start();
    try {
      monitor.join();
    } catch (InterruptedException e) {
      LOG.info(e.getMessage());
    }
    output.complete(dataFile, writerClient, allErrors);
    runState = RunState.COMPLETE;
    List<DataLoaderExecutionThread> invalidThreads = monitor.getInvalidLiveThreads();
    if (invalidThreads.size() == 0) {
      // If we have "stuck" threads then it is likely that the attempt
      // to force closure will also get stuck on the connection associated
      // with the stuck thread, so if we have any invalid threads we will
      // not attempt to close all connections down quickly
      closeAll(readOnlyClient);
      closeAll(writerClient);
    }
    for (int i = 0; i < invalidThreads.size(); i++) {
      ILoadingProcess dlet = invalidThreads.get(i);
      ErrorSummary temp = new ErrorSummary(m);
      temp.setAllLines(0);
      temp.setDbRequestTooLong(dlet.getNumberOfInvalidLines());
      allErrors.addErrors(temp);
    }
    allErrors.writeLogDetails(LOG);
    if (dispenser != null) {
      dispenser.terminate();
    }
    return allErrors;
  }


  private void loadFullCache(String destTable, final SqlMapClient readOnlyClient, final LoadingErrorList allErrors) {
    if (definition.isFullCache()) {
      String[] keyFields = definition.getAllKeyName();
      final ParameterCache cache = new ParameterCache(keyFields, 100000);
      String statementName = LoadingQueryWriter.getFullCacheQueryName(destTable);
      try {
        LOG.info("Populating full row cache for load on key fields: " + Strings.getListStringRecursive(keyFields, ", "));
        final int[] count = new int[1];
        readOnlyClient.queryWithRowHandler(statementName, new RowHandler<Map<String, Object>>() {
          public void handleRow(Map<String, Object> v) {
            cache.storeInCache(v, v.get(LoadingQueryWriter.getFullCacheValueName()));
            count[0]++;
          }
        });
        rowLookupCache = cache;
        LOG.info("Populated full row cache for load, " + count[0] + " records cached");
      } catch (SQLException e) {
        LOG.error("Loading failed at populating row cache");
        LOG.error("Error stacktrace:", e);
        allErrors.writeException(new Exception("Refresh data:", e), LOG);
        throw new RuntimeException("Error while trying to populate full row cache", e);
      }
    }
  }

  private void doKeyVerification(final File dataFile, Charset c) throws IOException {
    if (definition.isKeyVerificationScan()) {
      int[] lineNumbers = doFileKeyScan(dataFile, definition, c);
      if (lineNumbers != null) {
        throw new IOException("Duplicate keys found at lines " + Strings.getListString(lineNumbers, ", ") + " in file "
            + FileUtil.tryCanonical(dataFile));
      }
    }
  }
  private Properties getConnectionProperties(IDataSourceConfiguration dataSource) {
    return getConnectionProperties(dataSource == null ? null : dataSource.getName());
  }
  private Properties getConnectionProperties(String dataSource) {
    Properties connProps = getDataSourceConfiguration(dataSource, loadName);
    Properties props = new Properties();
    props.putAll(connProps);
    return props;
  }

  private void initLookupCaches(final SqlMapClient writerClient, final SqlMapClient readOnlyClient) {
    List<FieldDefinition> allFields = definition.getAllFields();
    for (FieldDefinition fieldDefinition : allFields) {
      if (fieldDefinition.isLookupField()) {
        buildLookupQueryCache(LoadingQueryWriter.getLookupQueryName(fieldDefinition.getFieldName()), fieldDefinition.getFieldName(), fieldDefinition.getLookupDefinition(), readOnlyClient, writerClient);
      }
    }
    GlobalFilter[] globalFilters = definition.getGlobalFilters();
    for (int i = 0; i < globalFilters.length; i++) {
      String lookupFilterName = globalFilters[i].getName();
      buildLookupQueryCache(LoadingQueryWriter.getLookupQueryName(lookupFilterName), lookupFilterName, globalFilters[i].getLookup(), readOnlyClient, writerClient);
    }
  }

  private URL writeSqlXmlContent(FileHandleManager m, ILoaderOutput output) throws IOException, MalformedURLException {
    final File generated = m.getTemporaryFile("sql", ".xml");
    BufferedWriter w = m.getWriter(generated);
    try {
      new LoadingQueryWriter(definition).writeQueriesToBuffer(output, w);
    } catch (InvalidLookupQueryException e2) {
      throw new RuntimeException(e2);
    } finally {
      w.close();
    }
    //
    URL xmlConfigFile = new URL("file://localhost/" + generated.getAbsolutePath());
    return xmlConfigFile;
  }

  private static IRecordDispenser<?> buildDispenserForFile(File file, LoadDefinition definition, int commitSize, Charset c)
      throws FileNotFoundException, IOException {
    return RecordDispenserFactory.getRecordDispenser(file, commitSize, definition.getXmlStartTag(),
        definition.getXQuery(), definition, c);
  }

  private void closeAll(final SqlMapClient readOnlyClient) {
    DataSource s = readOnlyClient.getDataSource();
    if (s instanceof SimpleDataSource) {
      ((SimpleDataSource) s).forceCloseAll();
    }
  }

  public boolean isBatch() {
    return mode == BATCH_MODE;
  }

  public boolean isSerial() {
    return mode == SERIAL_MODE;
  }

  /**
   * @param dispenser
   * @throws Exception
   */
  private void loadFromDispenser(long startTime, IRecordDispenser<String> dispenser, SqlMapClient transactionClient, SqlMapClient readClient,
      final Properties connectionProps, LoadingErrorList allSummaries, FileHandleManager fm, File sourceFile, ILoaderOutput o) throws SQLException, ConnectException, Exception {
    final List<String> currentLines = new ArrayList<String>();
    int startLine = dispenser.dispenseRecords(currentLines);
    int endLine = startLine + currentLines.size() - 1;
    final List<String> fields = Arrays.asList(definition.getAllFieldName());
    // Handler instances are not shared across threads, this way there is
    // no requirement for them to be threadsafe.
    Map<String, IFieldTextHandler> handlers = new HashMap<String, IFieldTextHandler>();
    List<FieldDefinition> all = definition.getAllFields();
    for (int i = 0; i < all.size(); i++) {
      FieldDefinition fd = all.get(i);
      String c = fd.getHandler();
      try {
        IFieldTextHandler h = (IFieldTextHandler) Reflect.invokeConstructor(c, new Class[0], new Object[0]);
        h.configure(fd.getHandlerArgs());
        handlers.put(fd.getFieldName(), h);
      } catch (ReflectionException e) {
        throw new RuntimeException("Unable to instantiate handler of type " + c);
      }
    }
    while (currentLines.size() > 0) {
      ((ILoadingProcess) Thread.currentThread()).setProcessingLines(currentLines);
      ErrorSummary errorSummary = new ErrorSummary(fm);
      final int recordCount = currentLines.size();
      errorSummary.setAllLines(recordCount);
      // allSummaries.add(errorSummary);
      try {
        processLinesAsTransaction(fields, readClient, transactionClient, currentLines, startLine,
            handlers, errorSummary, sourceFile, o);
      } catch (SQLException e) {
        errorSummary.setCommitFailed(recordCount);
        allSummaries.writeException(new Exception("Line " + startLine + " to " + "Line " + endLine, e), LOG);
        allSummaries.writeBadLines(currentLines, LOG);
        logLineError(startLine, endLine, e.getMessage(), e);
      } catch (Exception catchAll) {
        errorSummary.setUnknownError(recordCount);
        allSummaries.writeException(new Exception("Line " + startLine + " to " + "Line " + endLine, catchAll), LOG);
        allSummaries.writeBadLines(currentLines, LOG);
        logLineError(startLine, endLine, "Unexpected failure processing buffer " + catchAll.getMessage(), catchAll);
      }
      allSummaries.addErrors(errorSummary);

      if (!((ILoadingProcess) Thread.currentThread()).isValid()) {
        LOG.error(Thread.currentThread().getName()
            + " has become invalid because it failed to complete and will not continue.");
        break;
      }

      long millisElapsed = System.currentTimeMillis() - startTime;
      long secondsElapsed = (millisElapsed / 1000) + 1; // Adding 1 makes it
                                                        // very slightly
                                                        // inaccurate, but
                                                        // ensures no div by 0

      LOG.info("Processed up to " + endLine + " (~" + (endLine / secondsElapsed) + "/sec)");

      if (stopLoading) {
        LOG.warn("Loading thread " + Thread.currentThread().getName() + " is stopping.");
        break;
      }

      startLine = dispenser.dispenseRecords(currentLines);
      endLine = startLine + currentLines.size() - 1;
    }
  }

  /**
   * @return
   */
  private SqlMapClient getReadOnlyClient(String readerConfigFileName, Properties connProps, SqlMapClient writerClient) {
    if (shareReadAndWrite) {
      return writerClient;
    }
    return getClient(readerConfigFileName, connProps);

  }

  /**
   * @return
   */
  private SqlMapClient getWriterClient(String writerConfigFileName, Properties conProps) {
    final SqlMapClient client = getClient(writerConfigFileName, conProps);
    return client;
  }

  /**
   * @param generatedQueryFileLocation
   * @param sqlMapConfig
   * @return
   */
  protected SqlMapClient getClient(String sqlMapConfig, Properties p) {
    final SQLMapResource mr = new SQLMapResource("DataLoader", sqlMapConfig, p);
    final SqlMapClient client = mr.getSqlMapClient();
    return client;
  }

  /**
   * @param fields
   * @param reader
   * @param writer
   * @param linesRead
   * @param startLineNo
   * @param handlers
   * @return ErrorSummary
   * @throws SQLException
   * @throws RuntimeException
   */
  private void
      processLinesAsTransaction(List<String> fields, final SqlMapClient reader, final SqlMapClient writer,
          final List<String> linesRead, long startLineNo, final Map<String, IFieldTextHandler> handlers, ErrorSummary errorSummary, final File sourceFile, ILoaderOutput o) throws SQLException,
          RuntimeException {
    final List<String> filteredLines = new ArrayList<String>();
    final List<String> badLines = new ArrayList<String>();
    final List<Exception> exceptions = new ArrayList<Exception>();
    final List<String> statementNames = new ArrayList<String>();
    final List<Map<String, Object>> statementArgs = new ArrayList<Map<String, Object>>();
    long lineNo = startLineNo - 1;
    long dataIntegrityError = 0;
    long parsingFailed = 0;
    long filterOnFailed = 0;
    long unknownError = 0;
    final int noOfRecords = linesRead.size();
    for (Iterator<String> iter = linesRead.iterator(); iter.hasNext();) {
      Object line = iter.next();
      lineNo++;
      if (line == null) {
        continue;
      }
      if (definition.isHeader(line) || definition.isTrailer(line)) {
        errorSummary.addHeaderTrailerLine();
        continue;
      }
      Map<String, Object> m = new HashMap<String, Object>();
      try {
        String processedLine = processLine(line, lineNo, m, fields, reader, writer, handlers, o);
        if (processedLine != null) {
          statementNames.add(processedLine);
          statementArgs.add(m);
        }
      } catch (FilterConditionViolationException fe) {
        filterOnFailed++;
        logLineFilter(fe.getLineNumber(), fe.getMessage());
        if (printFilterRecordToFile)
          filteredLines.add(line + "");
      } catch (MissingLookupException e) {
        dataIntegrityError++;
        exceptions.add(new Exception("Line " + e.getLineNumber(), e));
        logLineError(e.getLineNumber(), e.getMessage());
        badLines.add(line + "");
      } catch (InvalidLineException e) {
        // Invalid lines are not fatal to the transaction, no attempt
        // would have been made to insert/update
        parsingFailed++;
        exceptions.add(new Exception("Line " + e.getLineNumber(), e));
        logLineError(e.getLineNumber(), e.getMessage());
        badLines.add(line + "");
      } catch (InvalidExecutionThreadException iete) {
        logLineError(startLineNo + 1, startLineNo + noOfRecords, "Execution thread is invalid. Caused by:", iete);
        errorSummary.setDbRequestTooLong(noOfRecords);
        break;
      } catch (Exception catchAll) {
        unknownError++;
        exceptions.add(new Exception("Line " + lineNo, catchAll));
        badLines.add(line + "");
        logLineError(lineNo, "Unexpected failure processing buffer " + catchAll.getMessage());
      }
    }
    errorSummary.setAllLines(linesRead.size());
    errorSummary.setDataIntegrityError(dataIntegrityError);
    errorSummary.setParsingError(parsingFailed);
    errorSummary.setFiltered(filterOnFailed);
    errorSummary.setUnknownError(unknownError);
    o.generateOutput(writer, linesRead, startLineNo, handlers, errorSummary, sourceFile, badLines, exceptions,
        statementNames, statementArgs);
    try {
      errorSummary.setFilteredLines(filteredLines);
    } catch (IOException e) {
      LOG.error("Failed to save filtered lines.", e);
    }
    getCommitListener().committed(getLoadName(), startLineNo, startLineNo + linesRead.size() - 1);
  }


  private void logLineFilter(long lineNumber, String message) {
    LOG.debug("Line " + lineNumber + " " + message);
  }

  private void logLineError(long lineNumber, String message) {
    LOG.error("Line " + lineNumber + " " + message);
  }

  private void logLineError(long startLineNumber, long endLineNumber, String message, Throwable e) {
    LOG.error("Line " + startLineNumber + " to " + "Line " + endLineNumber + " " + message, e);
  }



  private synchronized void buildLookupQueryCache(String lookupQueryName, String field, LookupDefinition lookupDefinition, final SqlMapClient reader,
      final SqlMapClient writer) {
    ILogProvider log = new DefaultLogProvider();
    log.register(new DataLoaderLookupLogClient());
    ILookupCache cache = new DefaultLookupCache(definition.getDestTable(), log);
    cache.configure(field, lookupDefinition, reader, writer);
    lookupCache.put(lookupQueryName, cache);
  }

  /**
   * @param writer
   * @param line
   * @param handlers
   * @throws InvalidExecutionThreadException
   * @throws InvalidLookupQueryException
   */
  private String processLine(Object line, long lineNo, final Map<String, Object> m, List<String> fields, final SqlMapClient reader,
      final SqlMapClient writer, Map<String, IFieldTextHandler> handlers, ILoaderOutput output) throws InvalidExecutionThreadException,
      InvalidLookupQueryException {

    int recordSize = 0;
    for (int i = 0; i < fields.size() && !definition.getLoadType().equals(RecordDispenserFactory.XML)
        && !definition.getLoadType().equals(RecordDispenserFactory.MSXLS); i++) {
      FieldDefinition fd = definition.getField(fields.get(i));
      String end = fd.getLocation().getEnd();
      int max = Integer.parseInt(end);
      if (max > recordSize)
        recordSize = max;
    }

    if (definition.isPadLine() && definition.getLoadType().equalsIgnoreCase(RecordDispenserFactory.FIXED_WIDTH)) {
      line = Strings.pad((String) line, recordSize);
    }
    Map<String, String> textMap = new HashMap<String, String>();
    String[] delimitedFields = getDelimitedFields(line, definition);
    for (Iterator<String> i = fields.iterator(); i.hasNext();) {
      String field = i.next();
      FieldDefinition fd = definition.getField(field);
      String valueText = getLineValueText(line, lineNo, delimitedFields, fd, definition);
      String[] filterValues = fd.getFilterOnValue();
      if (filterValues != null && filterValues.length > 0
          && !filterConditionsMet(valueText, filterValues, fd.getNullDefinition())) {
        if (fd.getFilterOnField() == null || fd.getFilterOnField().length == 0) {
          // For backward compatibility: No lookup filter, this is the final
          // result, reject it
          throw new FilterConditionViolationException(lineNo, field, valueText, Arrays.asList(fd.getFilterOnValue()));
        }
      }
      textMap.put(field, valueText);
    }

    for (int i = 0; i < fields.size(); i++) {
      FieldDefinition fd = definition.getField(fields.get(i));
      if (fd.getFilterOnField() != null && fd.getFilterOnField().length > 0) {
        Map<String, String> tempTextMap = new HashMap<String, String>();
        String field = fields.get(i);
        String valueText = textMap.get(field);
        tempTextMap.put(field, valueText);
        String[] filterFields = fd.getFilterOnField();
        for (int f = 0; f < filterFields.length; f++) {
          tempTextMap.put(filterFields[f], textMap.get(filterFields[f]));
        }
        processDirectFromFileFields(fields, tempTextMap, m, reader, writer, lineNo, handlers);
        processLookupFields(fields, tempTextMap, m, reader, writer, output, lineNo);
        boolean found = false;
        Object sourceFieldValue = m.get(field);
        for (int f = 0; f < filterFields.length; f++) {
          Object filterFieldValue = m.get(filterFields[f]);
          if ((sourceFieldValue == null && filterFieldValue == null)
              || (sourceFieldValue != null && sourceFieldValue.equals(filterFieldValue))) {
            found = true;
            break;
          }
        }
        if (!found) { // condition not met in lookup filtering
          if (fd.getFilterOnValue() == null || fd.getFilterOnValue().length == 0
              || !filterConditionsMet(valueText, fd.getFilterOnValue(), fd.getNullDefinition())) {
            throw new FilterConditionViolationException(lineNo, field, valueText + "/" + sourceFieldValue,
                Arrays.asList(fd.getFilterOnValue()) + " or value from field " + Arrays.asList(fd.getFilterOnField()));
          }
        }
      }
    }
    processDirectFromFileFields(fields, textMap, m, reader, writer, lineNo, handlers);
    // Here we do the global filters, after processing direct fields but before any lookup work is done
    GlobalFilter[] globalFilters = definition.getGlobalFilters();
    for (int i = 0; i < globalFilters.length; i++) {
      GlobalFilter filter = globalFilters[i];
      {
        LookupDefinition ld = filter.getLookup();
        Map<String, Object> lookupParams = new HashMap<String, Object>();
        lookupParams.putAll(m);
        ILookupCache cache = lookupCache.get(LoadingQueryWriter.getLookupQueryName(filter.getName()));
        Object value = cache.getLookupValue(lookupParams, lineNo, (ILoadingProcess) Thread.currentThread());
        if (value instanceof Boolean && ((Boolean)value).booleanValue()) {
          throw new FilterConditionViolationException(lineNo,  Strings.getListString(ld.getLookupQueryParams(), ","),  Strings.getListString(ld.getLookupQueryParams(), ","),
              "");
        }
      }
    }
    //
    processLookupFields(fields, textMap, m, reader, writer, output, lineNo);
    for (int i = 0; i < fields.size(); i++) {
      FieldDefinition fd = definition.getField(fields.get(i));
      if (!fd.isOptional() && m.get(fd.getFieldName()) == null) {
        throw new InvalidLineException(definition.getDestTable() + " field " + fd.getFieldName() + " can not be null, specified as not optional", lineNo);
      }
    }
    ILoadRowResolver resolver = getResolver(reader, output);
    ILoadingProcess thread = (ILoadingProcess) Thread.currentThread();
    String destTable = definition.getDestTable();
    return resolver.isRecordPresent(m, lineNo, thread) ? LoadingQueryWriter.getUpdateName(destTable)
        : LoadingQueryWriter.getInsertName(destTable);
  }

  public static String getLineValueText(Object line, long lineNo, String[] delimitedFields, FieldDefinition fd,
      LoadDefinition def) {
    String valueText = "";
    if (def.getLoadType().equalsIgnoreCase(RecordDispenserFactory.FIXED_WIDTH)) {
      valueText = getFixedWidthFieldText(line, lineNo, fd);
    } else if (def.getLoadType().equalsIgnoreCase(RecordDispenserFactory.DELIMITED)) {
      int start = new Integer(fd.getLocation().getStart()).intValue();
      if (start > 0) {
        valueText = delimitedFields[start - 1];
      } else if (start < 0 || start > delimitedFields.length) {
        throw new InvalidLineException("Field location invalid for field " + fd.getFieldName(), new Exception(
            "Field location can be between 1 - " + delimitedFields.length + " only"), lineNo);
      }
    } else if (def.getLoadType().equalsIgnoreCase(RecordDispenserFactory.XML) || def.getLoadType().equalsIgnoreCase(RecordDispenserFactory.MSXLS)) {
      @SuppressWarnings("unchecked")
      Map<String, String> fileFields = (Map<String, String>) line;
      valueText = (String) fileFields.get(fd.getLocation().getStart());
    }
    return valueText;
  }

  private static String getFixedWidthFieldText(Object line, long lineNo, FieldDefinition fd) {
    String text = "";
    int start = new Integer(fd.getLocation().getStart()).intValue() - 1;
    int end = new Integer(fd.getLocation().getEnd()).intValue();
    try {
      if (start > -1) {
        if (start >= end) {
          throw new InvalidLineException("Invalid start,end combination " + start + "," + end + " for "
              + fd.getFieldName(), null, lineNo);
        }
        text = ((String) line).substring(start, end).trim();
      }
    } catch (StringIndexOutOfBoundsException e) {
      throw new InvalidLineException("Line length invalid: " + ((String) line).length(), e, lineNo);
    }
    return text;
  }

  private ILoadRowResolver getResolver(SqlMapClient reader, ILoaderOutput output) {
    if (loadRowResolver == null) {
      if (output instanceof FixedWidthFileOutput) {
        loadRowResolver = new StaticLoadRowResolver(false);
      } else if (definition.getUpdateMode().equals(LoadDefinition.UPDATE_MODE)) {
        loadRowResolver = new StaticLoadRowResolver(true);
      } else if (definition.getUpdateMode().equals(LoadDefinition.INSERT_MODE)) {
        loadRowResolver = new StaticLoadRowResolver(false);
      } else {
        String selectName = LoadingQueryWriter.getSelectCountName(definition.getDestTable());
        loadRowResolver = new QueryLoadRowResolver(selectName, rowLookupCache, reader);
      }
    }
    return loadRowResolver;
  }

  private void processDirectFromFileFields(List<String> fields, Map<String, String> textMap, Map<String, Object> m, SqlMapClient reader, SqlMapClient writer,
      long lineNo, Map<String, IFieldTextHandler> handlers) {
    FieldDefinition currentlyParsing = null;
    try {
      for (Iterator<Map.Entry<String, String>> i = textMap.entrySet().iterator(); i.hasNext();) {
        Map.Entry<String, String> e = i.next();
        String field = e.getKey();
        IFieldTextHandlerContext context = new DefaultFieldTextHandlerContext(field, lineNo, getLoadName());
        FieldDefinition fd = definition.getField(field);
        if (fd.isLookupField()) {
          continue;
        }
        String valueText = textMap.get(field);
        Object value = null;
        if (valueText == null || valueText.equals("") || isNullByDefinition(valueText, fd.getNullDefinition())) {
          valueText = fd.getDefaultValue();
        }
        currentlyParsing = fd;
        IFieldTextHandler h = handlers.get(fd.getFieldName());
        value = h.getValue(valueText, textMap, fd.getDefaultValue(), context);
        m.put(field, value);
      }
    } catch (NumberFormatException e) {
      if (currentlyParsing == null) {
        throw e;
      } else {
        throw new FieldParsingFormatException("Failed to parse field " + currentlyParsing.getFieldName() + ": " + e.getMessage(), e);
      }
      
    }
  }

  private void processLookupFields(List<String> fields, Map<String, String> textMap, Map<String, Object> m, SqlMapClient reader, SqlMapClient writer,
      ILoaderOutput o, long lineNo) throws InvalidExecutionThreadException, InvalidLookupQueryException {
    for (Iterator<Entry<String, String>> i = textMap.entrySet().iterator(); i.hasNext();) {
      String field = (String) i.next().getKey();
      FieldDefinition fd = definition.getField(field);
      if (m.containsKey(field) || !fd.isLookupField()) {
        continue;
      } else if (o.isDeferLookupResolution(fd)) {
        LOG.debug("Skipped lookup for deferred lookup field " + fd + " for load " + getLoadName());
        m.put(fd.getFieldName(), textMap.get(fd.getFieldName()));
        continue;
      }
      processLookup(field, fields, textMap, m, reader, writer, lineNo);
    }
  }

  private void processLookup(String field, List<String> fields, Map<String, String> textMap, Map<String, Object> m, SqlMapClient reader, SqlMapClient writer,
      long lineNo) throws InvalidExecutionThreadException, InvalidLookupQueryException {
    FieldDefinition fd = definition.getField(field);
    if (!fd.isLookupField()) {
      return;
    }
    LookupDefinition ld = fd.getLookupDefinition();
    Object value = null;
    String valueText = (String) textMap.get(field);
    if (valueText == null || valueText.equals("") || isNullByDefinition(valueText, fd.getNullDefinition())) {
      valueText = fd.getDefaultValue();
    }
    List<String> qryParams = Arrays.asList(ld.getLookupQueryParams());
    for (int i = 0; i < qryParams.size(); i++) {
      String tmpField = (String) qryParams.get(i);
      if (isNotSelfLookup(tmpField, fd))
        processLookup(tmpField, fields, textMap, m, reader, writer, lineNo);
    }
    Map<String, Object> lookupParams = new HashMap<String, Object>();
    lookupParams.putAll(m);
    lookupParams.put(field, valueText);
    ILookupCache cache = lookupCache.get(LoadingQueryWriter.getLookupQueryName(field));
    value = cache.getLookupValue(lookupParams, lineNo, (ILoadingProcess) Thread.currentThread());
    m.put(field, value);
  }

  public static boolean isNotSelfLookup(String fieldNameInQuery, FieldDefinition lookupField) {
    return !fieldNameInQuery.equals(lookupField.getFieldName()) && !fieldNameInQuery.equalsIgnoreCase("VALUE")
        && !fieldNameInQuery.equalsIgnoreCase("CODE");
  }

  public void stopLoading() {
    stopLoading = true;
  }

  public boolean isRunning() {
    return runState == RunState.RUNNING;
  }

  private void setLoadName(String name) {
    if (name != null && name.trim().length() != 0)
      loadName = name;
  }

  public String getLoadName() {
    return loadName;
  }

  /**
   * @param line
   * @param d
   * @param length
   * @return NULL if not delimited, otherwise returns field texts
   */
  public static String[] getDelimitedFields(Object line, LoadDefinition d) {
    if (!d.getLoadType().equalsIgnoreCase(RecordDispenserFactory.DELIMITED)) {
      return null;
    }
    String delimiter = d.getDelimiter();
    if (delimiter == null) {
      throw new IllegalArgumentException("Load definition set to delimited but no delimiter defined");
    }
    String[] lines = ((String) line).split(delimiter);
    int length = d.getLastFieldStart();
    if (lines.length >= length) {
      return lines;
    } else {
      String[] tmpLines = new String[length];
      for (int i = 0; i < tmpLines.length; i++) {
        if (i < lines.length) {
          tmpLines[i] = lines[i];
        } else {
          tmpLines[i] = null;
        }
      }
      lines = tmpLines;
    }
    return lines;
  }

  private boolean filterConditionsMet(String valueText, String[] filter, String[] nullDefinition) {
    for (int i = 0; i < filter.length; i++) {
      if (filter[i].equalsIgnoreCase("NOT_NULL")) {
        if (valueText != null && valueText.length() > 0 && !isNullByDefinition(valueText, nullDefinition))
          return true;
        else
          return false;
      }

      if (filter[i].equalsIgnoreCase("NULL")) {
        if (valueText == null || valueText.length() == 0 || isNullByDefinition(valueText, nullDefinition))
          return true;
        else
          return false;
      }

      if (valueText.trim().equalsIgnoreCase(filter[i]))
        return true;
    }

    return false;
  }

  private boolean isNullByDefinition(String valueText, String[] nullDefinition) {
    if (nullDefinition == null || nullDefinition.length == 0)
      return false;
    return Arrays.asList(nullDefinition).contains(valueText);
  }

  private void checkConnection(SqlMapClient client) throws SQLException, ConnectException {
    Connection connection = client.getDataSource().getConnection();
    connection.close();
  }

  public static Properties getDataSourceConfiguration(String sourceName, String loadName) {
    IDataSourceConfiguration s = getDataSource(sourceName, loadName);
    Properties properties = s.getProperties();
    // check driver
    String driverName = null;
    try {
      driverName = properties.getProperty("DRIVER");
      if (driverName == null) {
        getLog().emit(DataLoaderLogClient.LOAD_NO_DRIVER, sourceName, loadName);
        throw new DriverNotFoundException(driverName);
      }
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      getLog().emit(DataLoaderLogClient.LOAD_BAD_DRIVER, e, driverName, sourceName, loadName);
      throw new DriverNotFoundException(driverName);
    }
    return properties;
  }

  private static IDataSourceConfigurationSet getDataSources() {
    String path = getDbSourceConfigPath();
    Properties connProps = Utilities.loadOrExit(path);
    IDataSourceConfigurationSet set = DataSourceUtil.getDataSourceConfigurations(connProps);
    return set;
  }

  public static String getDbSourceConfigPath() {
    return System.getProperty("ptrdc.properties", "server.properties");
  }

  private static void loadFromMultiLoadConfig(String[] args) throws Exception {
    Properties configProperties = null;
    try {
      configProperties = Utilities.loadFromPath(args[1]);
    } catch (IOException e1) {
      e1.printStackTrace();
      System.exit(1);
    }
    LoadConfigurationParser loadParser = new LoadConfigurationParser(configProperties);
    try {
      LoadConfigurationParser.printLoadConfigurations(loadParser);
    } catch (NotParsedException e) {
      e.printStackTrace();
      System.exit(1);
    } catch (LoadNotExistException e) {
      e.printStackTrace();
      System.exit(1);
    }
    List<String> sortedLoads = null;
    try {
      sortedLoads = loadParser.getSortedLoadNames();
    } catch (NotParsedException e) {
      e.printStackTrace();
      System.exit(1);
    }
    try {
    boolean exitOnFail = true;
    executeLoading(configProperties, loadParser, sortedLoads, exitOnFail);
    } catch (LoadNotExistException e) {
      e.printStackTrace();
      System.exit(1);
    } catch (DriverNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
  public static void executeLoading(Properties configProperties, LoadConfigurationParser loadParser, List<String> sortedLoads,
      ICommitListener cl) throws LoadNotExistException, ControlFileWaitExpiredException, Exception {
    executeLoading(configProperties, loadParser, sortedLoads, false, cl);
  }
  public static void executeLoading(Properties configProperties, LoadConfigurationParser loadParser,
      List<String> sortedLoads, boolean exitOnFail) throws LoadNotExistException, Exception, ControlFileWaitExpiredException {
    executeLoading(configProperties, loadParser, sortedLoads, exitOnFail, DUMMY_COMMIT_LISTENER);
  }
  private static void executeLoading(Properties configProperties, LoadConfigurationParser loadParser,
      List<String> sortedLoads, boolean exitOnFail, ICommitListener cl) throws LoadNotExistException, Exception,
      ControlFileWaitExpiredException {
    long timeout = Long.parseLong(configProperties.getProperty("dataloader.timeout", "0"));
    String dataSourceEncoding = configProperties.getProperty("dataloader.sourceEncoding", "UTF8");
    Charset sourceCharset = Charset.forName(dataSourceEncoding);
    for (int i = 0; i < sortedLoads.size(); i++) {
      String loadName = (String)sortedLoads.get(i);
      Map<String, Object> tmpMap = loadParser.getConfigsForLoad(sortedLoads.get(i));
      int threads = loadParser.getThreadCount(loadName);
      int commits = loadParser.getCommitSize(loadName);
      System.out.println("Starting load " + loadName);
      // Note the "trims" on the next 3 lines
      String fileL = ((String) tmpMap.get(LoadConfigurationParser.FILE_LOCATION)).trim();
      String fileN = ((String) tmpMap.get(LoadConfigurationParser.FILE_NAME)).trim();
      String waitFileExt = ((String) tmpMap.get(LoadConfigurationParser.WAIT_FILE_EXT)).trim();
      long waitDuration = ((Long) tmpMap.get(LoadConfigurationParser.WAIT_FOR_FILE)).longValue();
      DataLoader loader = new DataLoader(threads, commits, loadName);
      loader.setCommitListener(cl);
      File configFile = new File(((String) tmpMap.get(LoadConfigurationParser.CONFIG_FILE)).trim());
      if (!configFile.exists() || !configFile.canRead()) {
        if (exitOnFail) {
          System.err.println("Unable to find or read load configuration file " + FileUtil.tryCanonical(configFile) + " for load " + loadName);
          System.exit(1);
        } else {
          cl.ended(loadName, null);
          throw new RuntimeException("Unable to find or read load configuration file " + FileUtil.tryCanonical(configFile) + " for process "+loadName);
        }
      }
      try {
        loader.configureLoad(configFile, loadName);
      } catch (Exception e) {
        cl.ended(loadName, null);
        if (exitOnFail) {
          e.printStackTrace();
          System.exit(1);
        } else {
          throw e;
        }
      }
      String sourceName = (String) tmpMap.get(LoadConfigurationParser.DATA_SOURCE);
      IDataSourceConfiguration dataSource = DataLoader.getDataSource(sourceName, loadName);
      SimpleDateFormat fileNameDateFormatter = (SimpleDateFormat) tmpMap.get(LoadConfigurationParser.FILE_NAME_DATE_FORMAT);
      Date today = new Date();
      String fileName = Strings.ensureEndsWith(fileL, File.separator) + fileN + fileNameDateFormatter.format(today).trim();
      
      Date yesterday = DateParser.getPreviousDay(today);
      String diffingFileName = Strings.ensureEndsWith(fileL, File.separator) + fileN + fileNameDateFormatter.format(yesterday).trim();
      File diffingFile = new File(diffingFileName);
      File dataFile = new File(fileName);
      String isDiffing = ((String) tmpMap.get(LoadConfigurationParser.DIFF));
      // In the case where the date pattern in non-dynamic and resolves to the same file we don't want to diff.
      if (diffingFileName.equals(fileName) || ("false".equals(isDiffing))) {
        diffingFile = null;
      } else if (!diffingFile.exists()) {
        getLog().emit(DataLoaderLogClient.NO_DELTA_FILE, FileUtil.tryCanonical(diffingFile), loadName);
        diffingFile = null;
      }
      boolean isAllowingSortForDiff = true;
      if (diffingFile != null) {
        String isSortingInPlaceForDiff = ((String) tmpMap.get(LoadConfigurationParser.SORT));
        if (isSortingInPlaceForDiff != null && isSortingInPlaceForDiff.equals("false")) {
          isAllowingSortForDiff = false;
        }
      }
      Boolean disableFilterPrinting = (Boolean) tmpMap.get(LoadConfigurationParser.DISABLE_FILTER_FILE);
      loader.printFilterRecord(disableFilterPrinting);
      //
      String waitFileReplace = ((String) tmpMap.get(LoadConfigurationParser.WAIT_FILE_REPLACE));
      String waitFilePath = ((String) tmpMap.get(LoadConfigurationParser.WAIT_FILE_PATH));
      
      ControlFileManager controlFileManager;
      if (waitFilePath != null) {
        controlFileManager = new ControlFileManager(loadName, dataFile, waitFilePath, today);
      } else {
        controlFileManager = new ControlFileManager(loadName, dataFile, waitFileExt, waitFileReplace);
      }
      try {
        long durationInMilliSec = waitDuration * 60 * 1000;
        controlFileManager.waitForFile(durationInMilliSec);
      } catch (ControlFileWaitExpiredException e1) {
        cl.ended(loadName, null);
        if (exitOnFail) {
          System.err.println(e1.getMessage());
          System.exit(1);
        } else {
          throw e1;
        }
      }
      long startTime = System.currentTimeMillis();
      LoadingErrorList errors = null;
      try {
        ILoaderOutput output = loader.getOutput(loadName, loadParser, sourceCharset);
        cl.started(loadName);
        errors = loader.loadFile(fileName, dataSource, loadParser.getLogDirectory(), timeout, output, diffingFile, isAllowingSortForDiff, sourceCharset);
      } catch (Exception e) {
        cl.ended(loadName, errors);
        if (exitOnFail) {
        e.printStackTrace();
        System.exit(1);
        } else {
          throw e;
        }
      }
      loader.stopLoading();
      long endTime = System.currentTimeMillis();
      long duration = endTime - startTime;
      loader.logSummaries(loadName, errors, duration);
      cl.ended(loadName, errors);
    }
  }

  public static IDataSourceConfiguration getDataSource(String name, String loadName) {
    IDataSourceConfigurationSet dataSources = getDataSources();
    if (name == null) {
      return dataSources.getDefaultDataSource();
    }
    for (int i = 0; i < dataSources.getDataSources().length; i++) {
      if (name.equals(dataSources.getDataSources()[i].getName())) {
        return dataSources.getDataSources()[i];
      }
    }
    throw new RuntimeException("Data source requested '" + name + "' not found for load " + loadName);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out
          .println("Usage: java com.profitera.dc.DataLoader -c <load config properties file>");
      System.exit(1);
    }
    for (int i = 0; i < args.length; i++) {
      args[i] = args[i].trim();
    }
    loadFromMultiLoadConfig(args);
    // Interface for testing, we pass in and x instead of a c
    if (!args[0].equals("-x")) {
      System.exit(0);
    }
  }

  public void printFilterRecord(Boolean b) {
    printFilterRecordToFile = b;
  }

  // added for pcbatch
  public void logSummaries(String loadName, LoadingErrorList summary, long duration) {
    summary.logFailureSummaries(loadName, LOG, duration);
  }

  public int[] doFileKeyScan(File file, LoadDefinition def, Charset c) throws IOException {
    LoadFileKeyScanner scanner = new LoadFileKeyScanner(def);
    try {
      BigInteger collision = scanner.scan(buildDispenserForFile(file, def, KEY_SCAN_BATCH_SIZE, c));
      if (collision == null) {
        return null;
      }
      return scanner.findCollisions(collision, buildDispenserForFile(file, def, KEY_SCAN_BATCH_SIZE, c));
    } catch (InvalidLookupQueryException e) {
      throw new RuntimeException(e);
    }
  }

  public LoadDefinition getDefinition() {
    return definition;
  }
  
  public ILoaderOutput getOutput(String loadName, LoadConfigurationParser loadParser, Charset sourceCharset) throws NotParsedException {
    OutputType t = loadParser.getLoadOutput(loadName);
    LoadDefinition d = getDefinition();
    if (t.equals(OutputType.JDBC)) {
      return new DatabaseOutput(loadName, d, false);
    } else if (t.equals(OutputType.NATIVE)) {
      String copy = loadParser.getLoadNativeLoaderCopyCommand(loadName);
      String serverFileDir = loadParser.getLoadNativeLoaderServerPath(loadName);
      boolean isRecreating = loadParser.isLoadNativeLoaderRecreate(loadName);
      String dir = loadParser.getLogDirectory();
      NativeDatabaseLoaderOutput n = new NativeDatabaseLoaderOutput(loadName, d, isRecreating, copy, serverFileDir, dir, sourceCharset);
      n.setCommitSize(loadParser.getCommitSize(loadName));
      n.setThreadCount(loadParser.getThreadCount(loadName));
      n.setOutputFileDirectory(loadParser.getFileOutputPath(loadName));
      return n;
    } else {
      FixedWidthFileOutput n = new FixedWidthFileOutput(loadName, d);
      n.setThreadCount(loadParser.getThreadCount(loadName));
      n.setOutputFileDirectory(loadParser.getFileOutputPath(loadName));
      return n;
    }
  }
  private static ILogProvider getLog() {
    if (logger == null) {
      logger = new DefaultLogProvider();
      logger.register(new DataLoaderLogClient());
    }
    return logger;
  }
  public void setCommitListener(ICommitListener l) {
    if (l == null) {
      l = DUMMY_COMMIT_LISTENER;
    }
    this.commitListener = l;
  }
  protected ICommitListener getCommitListener() {
    return commitListener;
  }
}
