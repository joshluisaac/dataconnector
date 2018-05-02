package com.profitera.dc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.profitera.dataaccess.SqlMapProviderImpl;
import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.handler.MergeFieldsHandler;
import com.profitera.dc.parser.ExtractConfigurationParser;
import com.profitera.dc.parser.LoadConfigurationParser;
import com.profitera.dc.parser.XMLExtractDefinitionParser;
import com.profitera.dc.parser.exception.LoadNotExistException;
import com.profitera.dc.parser.exception.NotParsedException;
import com.profitera.util.ExceptionConverter;
import com.profitera.util.PassUtils;
import com.profitera.util.Strings;
import com.profitera.util.Utilities;

public class DataExtractor {
	
  private static final String MAP_FILE_PROPERTY = "MAP_FILE";
  public static final String DB_USER_PROPERTY = "USERNAME";
  public static final String DB_PASSWORD_PROPERTY = "DB_PASSWORD";
  public static final String DB_ENCRYPTED_PASSWORD_PROPERTY = "PASSWORD";
  
  private static final Log log = LogFactory.getLog(DataExtractor.class);
  //
  // Config fields:
  private int fetchSize = 1;
  private int skipSize = 0;
  private String writerName = "UNKNOWN";
  private static String CONN_CONFIG = "connConfig.xml";
  //
  private XMLExtractDefinitionParser edp = null;
  private DbRecordDispenser dispenser = null; 
  
  private Object lookupMutex = new Object();
  private Map lookupCache = new HashMap();
  
  private boolean stopWriting = false;
  private boolean isRunning = false;
  
  private BufferedWriter stacktraceFile = null;
  
  private BufferedWriter outputFile = null;
  
  private File parseDefinitionFile;
  private long recordWriten = 0;
  private Date extractionStartTime;
  
  public DataExtractor(String name) {
    setWriterName(name);
  }
  
  public DataExtractor(int fetch, String name) {
    fetchSize = fetch;
    setWriterName(name);
  }

  public DataExtractor(int fetch, int skip, String name) {
    fetchSize = fetch;
    skipSize = skip;
    setWriterName(name);
  }

  public DataExtractor(int fetch, int skip, String sqlMapCfg,  String name) {
    fetchSize = fetch;
    skipSize = skip;
    CONN_CONFIG = sqlMapCfg;
    setWriterName(name);
  }
  
  public void configureWriter(File configFile) throws Exception {
    parseDefinitionFile = configFile;
  	edp = new XMLExtractDefinitionParser();
    edp.parseDefinition(configFile);
  }
  
  public void configureWriter(Properties props) throws Exception {
  	edp = new XMLExtractDefinitionParser();
    edp.parseDefinition(props);
  }
  
  public List writeFile(String file, Properties connectionProps, String logDir) throws IOException, SQLException {
  	if (logDir == null || logDir.trim().length() == 0)
  		return writeFile(new File(file), connectionProps, null);
  	else
  		return writeFile(new File(file), connectionProps, new File(logDir));
  }

  public List writeFile(File file, final Properties connectionProps, File stackDir) throws IOException, SQLException {
  	setExtractionStartTime(Calendar.getInstance().getTime());
  	if (stackDir == null)
  		log.info("Stacktrace file directory is not provided, stacktrace file writing disabled.");
  	else if (!stackDir.exists() || !stackDir.canWrite())
  		log.info("Stacktrace file directory " + stackDir.getAbsolutePath() + " do not exist or not writable, stacktrace file writing disabled.");
  	else {
      stacktraceFile = new BufferedWriter(new FileWriter(new File(stackDir, getWriterName() + ".stacktrace"), true));
  	}

  	isRunning = true;
    final List allErrors = new ArrayList();

    if (edp == null)
      throw new IllegalStateException("ExtractDefinitionParser is not initialized");
    final File generated = File.createTempFile("sql", ".xml");
    BufferedWriter w = new BufferedWriter(new FileWriter(generated));
    w.write(edp.getHeader() + "\n");
    edp.writeQueriesToBuffer(w);
    w.write(edp.getFooter());
    w.close();

    URL xmlConfigFile = new URL("file://localhost/" + generated.getAbsolutePath());
    connectionProps.put(MAP_FILE_PROPERTY, xmlConfigFile.toString());
    
    log.info("Generated SQL configuration file URL: " + xmlConfigFile);
    final SqlMapProviderImpl sqlMapProvider = getSqlMapProvider(connectionProps);
    
    outputFile = new BufferedWriter(new FileWriter(file));
    dispenser = new DbRecordDispenser(sqlMapProvider, fetchSize, skipSize);
		dispenser.executeFetch(edp.getSelectName(), null);

		List extrationInfo = new ArrayList();
		extrationInfo.add(getExtractionStartTime());
		extrationInfo.add(getWriterName());
		if(getConfigFileName()!=null){
			extrationInfo.add(getConfigFileName());
		}
		
		if (edp.getOutputType().equalsIgnoreCase(RecordDispenserFactory.XML)){
			writeXMLRootStart(edp.getXMLRootTag());
		}else{
			// add Header if it's not xml output
			String header = edp.getOutputHeader(extrationInfo.toArray());
			if(header!=null && header.length()>0){
				writeHeader(header);
			}else{
				log.info("Header not defined. Skip header printing.");
			}
		}
				
    allErrors.addAll(writeFromDispenser(dispenser, connectionProps));
    
		if (edp.getOutputType().equalsIgnoreCase(RecordDispenserFactory.XML)){
			writeXMLRootEnd(edp.getXMLRootTag());
		}else{
			// add trailer if it's not xml output
			extrationInfo.add(recordWriten);
			String footer = edp.getOutputFooter(extrationInfo.toArray());
			if(footer!=null && footer.length()>0){
				writeFooter(footer);
			}else{
				log.info("Footer not defined. Skip footer printing.");
			}
		}

    isRunning = false;
    
    try {
    	outputFile.flush();
    	outputFile.close();
    } catch (IOException ioe) {
    	//nothing to do i guess
    }
    
    if (stacktraceFile != null){
	    try {
	    	stacktraceFile.flush();
	    	stacktraceFile.close();
	    } catch (Exception e1) {}
    }
    if (edp.isControlFileRequired()){
      File parentDir = file.getParentFile();
      String controlName = file.getName();
      controlName = edp.getControlFilePrefix() + controlName + edp.getControlFileSuffix();
      File controlFile = new File(parentDir.getAbsolutePath() + File.separator + controlName);
      if (controlFile.createNewFile()){
        log.info("Created control file at: " + controlFile.getAbsolutePath());
      } else {
        log.error("System did not create new control file at: " + controlFile.getAbsolutePath());
      }
    }

    return allErrors;
  }

  private SqlMapProviderImpl getSqlMapProvider(final Properties connectionProps) {
    return new SqlMapProviderImpl(CONN_CONFIG, connectionProps, "DataExtractor");
  }

	private String getConfigFileName() {
		if(parseDefinitionFile==null){
			return null;
		}
		return parseDefinitionFile.getName();
	}

  private void setExtractionStartTime(Date date){
  	extractionStartTime = date;
  }
  
	private Date getExtractionStartTime() {
		return extractionStartTime;
	}

  private List writeFromDispenser(DbRecordDispenser dispenser, final Properties connectionProps) throws IOException {
  	SqlMapProviderImpl sqlMapProvider = getSqlMapProvider(connectionProps);
  	final List currentRecords = new ArrayList();
    int startRecord = dispenser.dispenseRecords(currentRecords);
    final List fields = edp.getFieldsSortedByStartIndexes();
    final List allSummaries = new ArrayList();
    long writingFailed = 0;
    long unknownError = 0;
    final IFieldTextHandler[] handlers = edp.buildFieldHandlers();
    long startTime = System.currentTimeMillis();
    while (currentRecords.size() > 0) {
      Map errorSummary = new HashMap();
      int recordCount =  startRecord + currentRecords.size() - 1;
      try {
        errorSummary = processLinesAsTransaction(fields, handlers, sqlMapProvider, currentRecords, startRecord);
      } catch (SQLException e) {
        writingFailed = recordCount;
        writeException(new Exception("Record " + startRecord + " to record " + recordCount, e));
        logLineError(startRecord, recordCount, e.getMessage(), e);
      } catch (Exception catchAll) {
        unknownError = recordCount;
        writeException(new Exception("Record " + startRecord + " to record " + recordCount, catchAll));
        logLineError(startRecord, recordCount, "Unexpected failure processing buffer " + catchAll.getMessage(), catchAll);
      }
      unknownError += errorSummary.get(DbRecordDispenser.UNKNOWN_FAILED) == null ? 0 : ((Long)errorSummary.get(DbRecordDispenser.UNKNOWN_FAILED)).longValue();
      writingFailed += errorSummary.get(DbRecordDispenser.WRITES_FAILED) == null ? 0 : ((Long)errorSummary.get(DbRecordDispenser.WRITES_FAILED)).longValue();
      
      if (errorSummary.get(DbRecordDispenser.CAPTURED_EXCEPTIONS) != null)
        writeException((Collection)errorSummary.remove(DbRecordDispenser.CAPTURED_EXCEPTIONS));

      errorSummary.put(DbRecordDispenser.UNKNOWN_FAILED, new Long(unknownError));
      errorSummary.put(DbRecordDispenser.WRITES_FAILED, new Long(writingFailed));

      allSummaries.add(errorSummary);
      long millisElapsed = System.currentTimeMillis() - startTime;
      long secondsElapsed = (millisElapsed/1000) + 1; // Adding 1 makes it very slightly inacurate, but ensures no div by 0
      log.info("Processed up to " + recordCount + " (~" + (recordCount / secondsElapsed) + "/sec)");

      if (stopWriting) {
        log.warn("Writer thread " + Thread.currentThread().getName() + " is stopping.");
        break;
      }
      
      startRecord = dispenser.dispenseRecords(currentRecords);
    }
    return allSummaries;
  }

  private Map processLinesAsTransaction(List fields,
      IFieldTextHandler[] handlers, final SqlMapProviderImpl sqlMapProvider, List r, long startRecordNo) throws SQLException, RuntimeException {
    final List exceptions = new ArrayList();
    final List outputLines = new ArrayList();
    long recordNo = startRecordNo - 1;
    long dataIntegrityError = 0;
    long unknownError = 0;
    long writesFailed = 0;
    long queryFailed = 0;
    long conditionsFailed = 0;

    for (Iterator iter = r.iterator(); iter.hasNext();) {
      final Map record = (Map) iter.next();
      if (record == null)
        continue;
      recordNo++;
      // This processes "upper-casifies" all the records as they are returned
      // I'm not sure that this is very efficient, but it gets the job done
      List s = new ArrayList(record.keySet()); // Defensive copy to avoid concur mod Ex
      for (Iterator i = s.iterator(); i.hasNext();) {
        String key = (String) i.next();
        String upperKey = key.toUpperCase();
        if (!key.equals(upperKey)){
          record.put(upperKey, record.get(key));
        }
      }

      try {
        String outputLine = processLine(record, recordNo, fields, handlers, sqlMapProvider);
        if (outputLine != null && outputLine.length() != 0)
          outputLines.add(outputLine);
      } catch (FilterConditionViolationException fe){
        conditionsFailed++;
        logLineWarning(fe.getLineNumber(), fe.getMessage());
      } catch (MissingLookupException e){
        dataIntegrityError++;
        exceptions.add(new Exception("Record " + e.getLineNumber(), e));
        logLineError(e.getLineNumber(), e.getMessage());
      } catch (InvalidLineException e) {
        dataIntegrityError++;
        exceptions.add(new Exception("Record " + e.getLineNumber(), e));
        logLineError(e.getLineNumber(), e.getMessage());
      } catch (Exception catchAll) {
	      unknownError++;
        exceptions.add(new Exception("Record " + recordNo, catchAll));
	      logLineError(recordNo, "Unexpected failure processing records " + catchAll.getMessage());
      }
    }
    
    int written = writeRecords(outputLines, startRecordNo);
    writesFailed += outputLines.size() - written;
    
    Map errorSummary = new HashMap();
    errorSummary.put(DbRecordDispenser.WRITES_FAILED, new Long(writesFailed));
    errorSummary.put(DbRecordDispenser.QUERY_FAILED, new Long(queryFailed));
    errorSummary.put(DbRecordDispenser.UNKNOWN_FAILED, new Long(unknownError));
    errorSummary.put(DbRecordDispenser.CONDITIONS_FAILED, new Long(conditionsFailed));
    errorSummary.put(DbRecordDispenser.ALL_RECORDS, new Long(recordNo - (startRecordNo - 1)));
    errorSummary.put(DbRecordDispenser.CAPTURED_EXCEPTIONS, exceptions);
    return errorSummary;
  }
  
  private void writeXMLRootStart(String tag){
  	try {
    	outputFile.write("<" + tag + ">\n");
			outputFile.flush();
		} catch (IOException e) {
			logLineError(0, "Writing xml root tag start failed", e);
		}
  }

  private void writeXMLRootEnd(String tag){
  	try {
    	outputFile.write("</" + tag + ">");
			outputFile.flush();
		} catch (IOException e) {
			logLineError(0, "Writing xml root tag end failed", e);
		}
  }
  
  private void writeHeader(String header){
  	try {
  		outputFile.write(header);
  		outputFile.newLine();
  		outputFile.flush();
		} catch (IOException e) {
			logLineError(0, "Writing header failed", e);
		}
  }
  
  private void writeFooter(String footer){
  	try {
  		outputFile.write(footer);
  		outputFile.flush();
		} catch (IOException e) {
			logLineError(0, "Writing footer failed", e);
		}
  }
  
  private synchronized int writeRecords(List lines, long startRecordNo){
  	int written = 0;
  	for (int i = 0; i < lines.size(); i++){
			try {
				outputFile.write((String)lines.get(i));
				outputFile.newLine();
				written++;
				recordWriten++;
			} catch (IOException e) {
				logLineError(startRecordNo, "Writing failed", e);
			}
			startRecordNo++;
  	}
  	try {
			outputFile.flush();
		} catch (IOException e) {
			//no reason for exception here.. we just printstacktrace lah..
			e.printStackTrace();
		}
  	return written;
  }

  private void logLineWarning(long recordNo, String message) {
    log.warn(Strings.leftPad("Record " + recordNo, 8) + " " + message);
  }

  private void logLineError(long recordNo, String message) {
    log.error(Strings.leftPad("Record " + recordNo, 8) + " " + message);
  }

  private void logLineError(long recordNo, String message, Exception e) {
    log.error(Strings.leftPad("Record " + recordNo, 8) + " " + message, e);
  }
  private void logLineError(long startRecordNo, long endRecordNo, String message, Throwable e) {
    log.error(Strings.leftPad("Record " + startRecordNo, 8) 
        + " to " + "record " + endRecordNo 
        + " " + message, e);
  }

  private Object getLookupValue(SqlMapProviderImpl sqlMapProvider, int index, String field, Map params, long line) {
  	synchronized (lookupMutex) {
	    String lookupQueryName = edp.getLookupQueryName(index);
	    String lookupCond = new String();
	    List lookupConds = edp.getLookupQueryParams(index);
	    for (int i = 0; i < lookupConds.size(); i++)
	      lookupCond += "" + lookupConds.get(i) + "=" + params.get(lookupConds.get(i)) + ";";
	
	    Map cache = getLookupQueryCache(edp.getLookupQueryName(index));
	    Object lookedUp = getFromCache(cache, index, params);
	
	    if (lookedUp != null){
	      log.debug("Lookup value for " + field + " [" + lookupCond + "]" + " resolved to " + lookedUp + " from cache");
	      return lookedUp;
	    }
	
	    try {
	      lookedUp = sqlMapProvider.queryObject(lookupQueryName, params);
	    } catch (SQLException e1) {
	      throw new InvalidLineException("Lookup query for " + field + " invalid: " + lookupQueryName, e1, line);
	    }
	    if (lookedUp == null) {
	      if (!edp.isOptionalLookup(index))
	        throw new MissingLookupException(field, lookupCond, params.get(field), line);
	      else
	        log.warn("Lookup value for optional " + field + " [" + lookupCond + "]" + " not found.");
	    } else
	      log.debug("Lookup value for " + field + " [" + lookupCond + "]" + " resolved to " + lookedUp);
	    storeInCache(cache, index, params, lookedUp);
	    return lookedUp;
  	}
  }

  private Map getLookupQueryCache(String lookupQueryName) {
	  Map cache = (Map) lookupCache.get(lookupQueryName);
	  if (cache == null) {
	    cache = new HashMap();
	    lookupCache.put(lookupQueryName, cache);
	  }
	  return cache;
  }
  
  private Object getFromCache(Map cache, int index, Map params){
    List lookupParams = edp.getLookupQueryParams(index);
	Map tmpCache = cache;
	for (int i = 0; i < lookupParams.size(); i++){
	  String lookupParam = (String)lookupParams.get(i);
	  Object paramValue = params.get(lookupParam);
	  if (i != lookupParams.size() - 1){
	    tmpCache = (Map)tmpCache.get(lookupParam + "=" + paramValue);
	    if (tmpCache == null)
	      return null;
	  }
	  else
	    return tmpCache.get(lookupParam + "=" + paramValue);
	}
    return null;
  }

  private void storeInCache(Map cache, int index, Map params, Object value){
    List lookupParams = edp.getLookupQueryParams(index);
    Map tmpCache = cache;
		for (int i = 0; i < lookupParams.size(); i++){
		  String lookupParam = (String)lookupParams.get(i);
		  Object paramValue = params.get(lookupParam);
		  if (i != lookupParams.size() - 1){
		    if (tmpCache.get(lookupParam + "=" + paramValue) == null)
		      tmpCache.put(lookupParam + "=" + paramValue, new HashMap());
		    tmpCache = (Map)tmpCache.get(lookupParam + "=" + paramValue);
		  }
		  else
		    tmpCache.put(lookupParam + "=" + paramValue, value);
		}
  }

  private void processNonLookupFields(List fields, Map record, IFieldTextHandler[] handlers, long recordNo){
  	
  	for (Iterator i = fields.iterator(); i.hasNext();) {
      String field = (String) i.next();
    	int index = edp.getFieldIndex(field);
      int length = edp.getFieldLength(index);
      if (handlers[index] instanceof MergeFieldsHandler)
      	continue;
      String textValue = "";
      try{
      	textValue = handlers[index].getReverseValue(record.get(field), record, edp.getDefaultValue(index), length);
      }catch(Exception e){
      	Object o = record.get(field);
      	String message = handlers[index].getClass().getName()+" failed for "+field+ " with value '"+o+"'";
      	if (o != null) {
      	  message = message + " of type "+ o.getClass().getName();
      	}
      	throw new InvalidLineException(message, recordNo);
      }
      if (edp.getFilterOn(index) != null && !filterConditionsMet(textValue, edp.getFilterOn(index), edp.getNullDefinition(index)))
        throw new FilterConditionViolationException(recordNo, field, textValue, edp.getFilterOn(index));
      record.put(field, textValue);
    }
  }
  
  private void processSpecialFields(List fields, Map record, IFieldTextHandler[] handlers, long recordNo){
  	processMergeFields(fields, record, handlers, recordNo);
  }
  
  private void processMergeFields(List fields, Map record, IFieldTextHandler[] handlers, long recordNo){
  	for (Iterator i = fields.iterator(); i.hasNext();) {
      String field = (String) i.next();
    	int index = edp.getFieldIndex(field);
      int length = edp.getFieldLength(index);
      if (!(handlers[index] instanceof MergeFieldsHandler))
      	continue;
      String textValue = "";
      try{
      	 textValue = handlers[index].getReverseValue(record.get(field), record, edp.getDefaultValue(index), length);
      }catch(Exception e){
      	Object o = record.get(field);
      	throw new InvalidLineException(handlers[index].getClass().getName()+" cannot be used for "+field+ " because value "+o+" is type of "+o.getClass().getName(), recordNo);
      }
      if (edp.getFilterOn(index) != null && !filterConditionsMet(textValue, edp.getFilterOn(index), edp.getNullDefinition(index)))
        throw new FilterConditionViolationException(recordNo, field, textValue, edp.getFilterOn(index));
      record.put(field, textValue);
    }
  }

  private String processLine(Map record, long recordNo, List fields,
      IFieldTextHandler[] handlers, final SqlMapProviderImpl sqlMapProvider) {

  	processLookupFields(fields, record, sqlMapProvider, recordNo);
  	processNonLookupFields(fields, record, handlers, recordNo);
  	processSpecialFields(fields, record, handlers, recordNo);
  	
  	if (edp.getOutputType().equalsIgnoreCase(RecordDispenserFactory.FIXED_WIDTH))
  		return processFixedLine(record, recordNo, fields, handlers, sqlMapProvider);
  	else if (edp.getOutputType().equalsIgnoreCase(RecordDispenserFactory.XML))
  		return processXMLLine(record, recordNo, fields, handlers, sqlMapProvider);
  	else if (edp.getOutputType().equalsIgnoreCase(RecordDispenserFactory.DELIMITED))
  		return processDelLine(record, recordNo, fields, handlers, sqlMapProvider, false);
    else if (edp.getOutputType().equalsIgnoreCase(RecordDispenserFactory.DELIMITED_KEY_VALUE))
      return processDelLine(record, recordNo, fields, handlers, sqlMapProvider, true);
  	else
  		return "Output file type '" + edp.getOutputType() + "' is invalid";
  }

  private String processFixedLine(Map record, long recordNo, List fields,
      IFieldTextHandler[] handlers, final SqlMapProviderImpl sqlMapProvider) {

    int recLength = edp.getRecordSize();
    StringBuffer line = new StringBuffer(Strings.filler(recLength));

    for (Iterator i = fields.iterator(); i.hasNext();) {
      String field = (String) i.next();
    	int index = edp.getFieldIndex(field);
    	if (edp.isExternal(index))
    		continue;
      String textValue = (String)record.get(field);
      int start = Integer.parseInt(edp.getStartIndex(index));
      int end = Integer.parseInt(edp.getEndIndex(index));
      line.replace(start - 1, end, textValue);
    }
    return line.toString();
  }

  private String processXMLLine(Map record, long recordNo, List fields,
      IFieldTextHandler[] handlers, final SqlMapProviderImpl sqlMapProvider) {

    String line = new String();
    line += "\t"; //simple indentation, one tab per record
    line += "<" + edp.getXMLStartTag() + ">"; //start of record tag

    for (Iterator i = fields.iterator(); i.hasNext();) {
      String field = (String) i.next();
    	int index = edp.getFieldIndex(field);
    	if (edp.isExternal(index))
    		continue;
      String textValue = (String)record.get(field);
      if (textValue != null){
      	textValue = textValue.replaceAll("&", "&amp;");
      	textValue = textValue.replaceAll("<", "&lt;");
      }
      String tag = edp.getStartIndex(index);
      line += "<" + tag + ">" + textValue + "</" + tag + ">";
    }
    line += "</" + edp.getXMLStartTag() + ">"; //end of record tag
    return line;
  }
  
  private String processDelLine(Map record, long recordNo, List fields,
      IFieldTextHandler[] handlers, final SqlMapProviderImpl sqlMapProvider,  boolean isKeyValuePair) {
    int previousPos = 0;
    String line = new String();
    for (Iterator i = fields.iterator(); i.hasNext();) {
      String field = (String) i.next();
    	int index = edp.getFieldIndex(field);
    	if (edp.isExternal(index))
    		continue;
      String textValue = (String)record.get(field);
      int start = Integer.parseInt(edp.getStartIndex(index));
      while (start - previousPos > 1){
      	line += edp.getDelimiter();
      	previousPos++;
      }
      if (isKeyValuePair){
        line += field.toLowerCase() + "=";
      }
      if (textValue != null)
      	line += textValue;
    }
    return line;
  }

  private void processLookupFields(List fields, Map record, SqlMapProviderImpl sqlMapProvider,  long recordNo){
    for (Iterator i = fields.iterator(); i.hasNext();) {
      String field = (String) i.next();
      if (record.containsKey(field) || !edp.hasLookupQuery(edp.getFieldIndex(field)))
        continue;
      int lookupFieldIndex = edp.getFieldIndex(field);
      if (lookupFieldIndex == -1){
        throw new IllegalArgumentException("Lookup references field named '" + field + "' which does not exist");
      }
      processLookup(lookupFieldIndex, field, fields, record, sqlMapProvider, recordNo);
    }
  }

  private void processLookup(int index, String field, List fields, Map record, SqlMapProviderImpl sqlMapProvider, long recordNo){
    if (!edp.hasLookupQuery(index))
      return;
    Object value = null;
    List qryParams = edp.getLookupQueryParams(index);
    for (int i = 0; i < qryParams.size(); i++){
      String tmpField = (String)qryParams.get(i);
      if (!tmpField.equals(field) && !record.containsKey(tmpField)){
        int tmpFieldIndex = edp.getFieldIndex(tmpField);
        if (tmpFieldIndex == -1){
          throw new IllegalArgumentException("Lookup query for '" + field + "' references undefined field " + tmpField);
        }
        processLookup(tmpFieldIndex, tmpField, fields, record, sqlMapProvider, recordNo);
      }
    }
    Map lookupParams = new HashMap();
    lookupParams.putAll(record);
    value = getLookupValue(sqlMapProvider, index, field, lookupParams, recordNo);
    record.put(field, value);
  }
  
  public void stopWriting(){
    stopWriting = true;
  }
  
  public boolean isRunning() {
    return isRunning;
  }
  
  public int getNumberOfRecordsProcessed(){
    if (dispenser != null)
      return dispenser.getCurrentRecordPosition();
    return 0;
  }
  
  private void setWriterName(String name) {
    if (name != null && name.trim().length() != 0)
      writerName = name;
  }
  
  public String getWriterName(){
    return writerName;
  }
  
  private boolean filterConditionsMet(String valueText, String filter, String nullDefinition){
    String[] filterConditions = filter.split(";");
    for (int i = 0; i < filterConditions.length; i++){
      if (filterConditions[i].equalsIgnoreCase("NOT_NULL"))
        if (valueText != null && valueText.length() > 0 && !isNullByDefinition(valueText, nullDefinition))
        	return true;

      if (filterConditions[i].equalsIgnoreCase("NULL"))
          if (valueText == null || valueText.length() == 0 || isNullByDefinition(valueText, nullDefinition))
          	return true;
      
      if (valueText.trim().equalsIgnoreCase(filterConditions[i]))
      	return true;
    }
    return false;
  }
  
  private boolean isNullByDefinition(String valueText, String nullDefinition){
	  if (nullDefinition == null || nullDefinition.trim().length() == 0)
		  return false;
	  return Arrays.asList(nullDefinition.split(";")).contains(valueText);
  }
  
  private static String getTextPassword(Properties props){
  	String encryptedPaswordFileLoc = props.getProperty(DB_ENCRYPTED_PASSWORD_PROPERTY);
  	File encryptedPasswordFile = new File(encryptedPaswordFileLoc);
    final String file = props.getProperty(PassUtils.DB_CRYPTKEY);
    if (null == file) throw new RuntimeException("Database crypt key file is not specified in server config!");
    String key = null;
    try {
      key = readCryptFile(file);
    } catch (Exception ex) {
      throw new RuntimeException("Unable to read encryption key from file " + file + "; Error: " + ex.getMessage(), ex);
    }
    BufferedReader passwordReader = null;
  	try {
  		passwordReader = new BufferedReader(new FileReader(encryptedPasswordFile));
			final String encryptedPassword = passwordReader.readLine();
			return PassUtils.desDecrypt(encryptedPassword, key);
  	}	catch (FileNotFoundException e) {
  		throw new RuntimeException("Password File " + encryptedPasswordFile.getAbsolutePath() + " was not found!");
  	}
	  catch (IOException e) {
	  	throw new RuntimeException("Password File is corrupt! Could not read password");
	  }
	  finally {
	  	if (null != passwordReader) {
	  		try{
	  			passwordReader.close();
	  		} catch (IOException e) {
	  			e.printStackTrace();
	  		}
	  	}
	  }
  }
  
  private static String readCryptFile(String file)
  {
    final File keyFile = new File(file);
    BufferedReader r = null;
    try {
        final FileReader fi = new FileReader(keyFile);
        r = new BufferedReader(fi);
        return r.readLine();
    } catch (FileNotFoundException e) {
        throw ExceptionConverter.wrap(e);
    } catch (IOException e) {
        throw ExceptionConverter.wrap(e);
    } finally {
        try {
            if (null != r) r.close();
        } catch (IOException e) {e.printStackTrace();}
    }
  }

  public static void main(String[] args){
    if (args.length != 6 && args.length != 7 && args.length != 2) {
      System.out.println("Usage: java com.profitera.dc.DataExtractor <mapping properties> <handoff file> <fetch size> <skip size> <db user> <db password> [<log dir>] | -c <load config file>");
      System.exit(1);
    }
    ExtractConfigurationParser ecp = null;
    File configFile = null;
    String loadName = null;
    String fileName = null;
    int fetch = 1;
    int skip = 0;
    String logDir = null;
    if (args.length == 2){
    	Properties cfgProps = null;
			try {
				cfgProps = Utilities.loadFromPath(args[1]);
			} catch (IOException e1) {
				e1.printStackTrace();
				System.exit(1);
			}
			logDir = cfgProps.getProperty("dataextractor.logDir");
    	ecp = new ExtractConfigurationParser(cfgProps);
    	boolean parsed = ecp.parseConfiguration();
    	if (!parsed){
    		System.out.println("Unable to parse configuration file " + args[1]);
    		System.exit(1);
    	}
    	List sortedLoads = null;
			try {
				sortedLoads = ecp.getSortedExtractNames();
			} catch (NotParsedException e) {
				e.printStackTrace();
				System.exit(1);
			}
      try {
      	ExtractConfigurationParser.printExtractConfigurations(ecp);
			} catch (NotParsedException e) {
				e.printStackTrace();
				System.exit(1);
			} catch (LoadNotExistException e) {
				e.printStackTrace();
				System.exit(1);
			}
      for (int i = 0; i < sortedLoads.size(); i++){
        Map tmpMap = null;
				try {
					tmpMap = ecp.getConfigsForExtract((String)sortedLoads.get(i));
				} catch (NotParsedException e) {
					e.printStackTrace();
					System.exit(1);
				} catch (LoadNotExistException e) {
					e.printStackTrace();
					System.exit(1);
				}
        loadName = (String) tmpMap.get(ExtractConfigurationParser.NAME);
        String dataSource = (String)tmpMap.get(ExtractConfigurationParser.DATA_SOURCE);
        Properties props = DataLoader.getDataSourceConfiguration(dataSource, loadName);
        String dbUser = props.getProperty(DataExtractor.DB_USER_PROPERTY);
        String dbPassword = getTextPassword(props);
        props.put(DataExtractor.DB_USER_PROPERTY, dbUser);
        props.put(DataExtractor.DB_PASSWORD_PROPERTY, dbPassword);
        skip = ((Integer)tmpMap.get(ExtractConfigurationParser.SKIP)).intValue();
        fetch = ((Integer)tmpMap.get(ExtractConfigurationParser.FETCH)).intValue();
        String fileL = (String)tmpMap.get(ExtractConfigurationParser.FILE_LOCATION);
        String fileN = (String)tmpMap.get(ExtractConfigurationParser.FILE_NAME);
        String date = ((SimpleDateFormat)tmpMap.get(ExtractConfigurationParser.FILE_NAME_DATE_FORMAT)).format(new Date());
        String configFileName = (String)tmpMap.get(LoadConfigurationParser.CONFIG_FILE);
                
        fileName = Strings.ensureEndsWith(fileL, "/") + fileN + date;
        		
        configFile = new File(configFileName);
      	if (!configFile.exists() || !configFile.canRead()){
      		System.err.println("Unable to find or read load configuration file " + configFileName + " for extraction " + loadName);
      		System.exit(1);
      	}
		
        System.out.println("Starting writer " + loadName);
  	    DataExtractor ld = new DataExtractor(fetch, skip, loadName);
  	    try {
					ld.configureWriter(configFile);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}

				List errors = null;
  	    try {
					errors = ld.writeFile(fileName, props, logDir);
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				} catch (SQLException e) {
					e.printStackTrace();
					System.exit(1);
				}
				ld.stopWriting();
        System.out.println("Finished writer " + loadName);
  	    ld.logFailureSummaries(errors);
      }
    } else {
    	configFile = new File(args[0]);
    	if (!configFile.exists() || !configFile.canRead()){
    		System.err.println("Unable to find or read load configuration file " + args[0]);
    		System.exit(1);
    	}
	    fileName = args[1];
	    loadName = Strings.removeFileNameExtension(Strings.getLastWord(fileName, File.separator));
	    // Single extraction, not data source specification allowed
	    Properties props = DataLoader.getDataSourceConfiguration(null, loadName);
      String dbUser = props.getProperty(DataExtractor.DB_USER_PROPERTY);
      String dbPassword = getTextPassword(props);
      props.put(DataExtractor.DB_USER_PROPERTY, dbUser);
      props.put(DataExtractor.DB_PASSWORD_PROPERTY, dbPassword);
	    skip = Integer.parseInt(args[3]);
	    fetch = Integer.parseInt(args[2]);
	    logDir = (args.length == 7 ? args[6] : null);
	    DataExtractor ld = new DataExtractor(fetch, skip, loadName);
	    try {
				ld.configureWriter(configFile);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
	    List errors = null;
			try {
				errors = ld.writeFile(fileName, props, logDir);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			} catch (SQLException e) {
				e.printStackTrace();
				System.exit(1);
			}
	    ld.logFailureSummaries(errors);
    }
  }

  private void logFailureSummary(Map m) {
    Number conditionFails = (Number) m.get(DbRecordDispenser.CONDITIONS_FAILED);
    Number queryFails = (Number) m.get(DbRecordDispenser.QUERY_FAILED);
    Number unknownFails = (Number) m.get(DbRecordDispenser.UNKNOWN_FAILED);
    Number writeFails = (Number) m.get(DbRecordDispenser.WRITES_FAILED);
    Number records = (Number) m.get(DbRecordDispenser.ALL_RECORDS);
    log.info(conditionFails + " of " + records + " failed due to conditional errors");
    log.info(queryFails + " of " + records + " failed due to query errors");
    log.info(unknownFails + " of " + records + " failed due to unknown errors");
    long all = conditionFails.longValue()  + queryFails.longValue() + unknownFails.longValue();
    log.info(all + " of " + records + " total failures");
    if (writeFails.longValue() > 0)
    	log.info(writeFails + " writes failed when persisting data");
  }

  private void logFailureSummaries(List failureSummaryMaps){
    long conditionFails = 0;
    long queryFails = 0;
    long unknownFails = 0;
    long writeFails = 0;
    long records = 0;
    for (Iterator i = failureSummaryMaps.iterator(); i.hasNext();) {
      Map m = (Map) i.next();
      conditionFails += ((Number) m.get(DbRecordDispenser.CONDITIONS_FAILED)).longValue();
      queryFails += ((Number) m.get(DbRecordDispenser.QUERY_FAILED)).longValue();
      unknownFails += ((Number) m.get(DbRecordDispenser.UNKNOWN_FAILED)).longValue();
      writeFails += ((Number) m.get(DbRecordDispenser.WRITES_FAILED)).longValue();
      records += ((Number) m.get(DbRecordDispenser.ALL_RECORDS)).longValue();
    }
    Map total = new HashMap();
    total.put(DbRecordDispenser.CONDITIONS_FAILED, new Long(conditionFails));
    total.put(DbRecordDispenser.QUERY_FAILED, new Long(queryFails));
    total.put(DbRecordDispenser.UNKNOWN_FAILED, new Long(unknownFails));
    total.put(DbRecordDispenser.WRITES_FAILED, new Long(writeFails));
    total.put(DbRecordDispenser.ALL_RECORDS, new Long(records));
    logFailureSummary(total);
  }
  
  private void writeException(Exception theError){
  	List errors = new ArrayList();
  	errors.add(theError);
  	writeException(errors);
  }
  
  private void writeException(Collection collectionOfErrors){
  	writeException(new ArrayList(collectionOfErrors));
  }
  
  private synchronized void writeException(List errors) {
    
    if (errors == null || errors.size() == 0 || stacktraceFile == null)
      return;
    
    try {
    	PrintWriter writer = new PrintWriter(stacktraceFile, true);
    	for (int i = 0; i < errors.size(); i++){
    		Exception theError = (Exception)errors.get(i);
    		theError.printStackTrace(writer);
    	}
    } catch (Exception ex) {
      log.warn("Error while trying to write stacktrace log file");
      log.warn("Error received: " + ex);
      log.warn("Error stacktrace:", ex);
    }
  }
  
  // added for pcbatch
  public void logSummaries(List summary) {
	  logFailureSummaries(summary);
  }
}