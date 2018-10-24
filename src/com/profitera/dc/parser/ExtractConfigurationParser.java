package com.profitera.dc.parser;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.profitera.dc.parser.exception.BatchNotExistException;
import com.profitera.dc.parser.exception.LoadNotExistException;
import com.profitera.dc.parser.exception.NotParsedException;
import com.profitera.util.MapComparator;
import com.profitera.util.Utilities;

public class ExtractConfigurationParser {
  
  private final static Log log = LogFactory.getLog(ExtractConfigurationParser.class);
  
  private static final String NAMES_SEPARATOR = ";";

  public static final String MODULE_NAME = "dataextractor";

  public static final String BATCH_NAMES = "batchNames";

  public static final String EXTRACT_NAMES = "extractNames";
  public static final String START_TIME = "startTime";
  public static final String START_HOUR = "startHour";
  public static final String START_MINUTE = "startMinute";
  
  public static final String SEQUENCE = "sequence";
  public static final String NAME = "name";
  public static final String FETCH = "fetch";
  public static final String SKIP = "skip";
  public static final String FILE_LOCATION = "fileLocation";
  public static final String FILE_NAME = "fileName";
  public static final String FILE_NAME_DATE_FORMAT = "fileNameDateFormat";
  public static final String CONFIG_FILE = "configFile";
  public static final String DEPENDS = "depends";
  public static final String WAIT_FOR_FILE = "waitForFile";
  public static final String WAIT_FILE_EXT = "waitFileExt";
  public static final String DATA_SOURCE = "dataSource";

  public static final String DE_STACKTRACE_DIR = "DataExtractorStacktraceDir";
  public static final String DE_BADFILE_DIR = "DataExtractorBadFileDir";

  public static final String[] BATCH_PROPERTIES = {EXTRACT_NAMES, START_TIME};
  public static final Object[] BATCH_PROPERTIES_TYPE = {String.class, Date.class};

  public static final String[] EXTRACT_PROPERTIES =
  {SEQUENCE, NAME, FETCH,
  SKIP, FILE_LOCATION, FILE_NAME, FILE_NAME_DATE_FORMAT, CONFIG_FILE, DEPENDS};
  public static final Object[] EXTRACT_PROPERTIES_TYPE = { Integer.class,
      String.class, Integer.class, Integer.class, String.class, String.class,
      String.class, String.class, String.class };

  private Map batchExtract = new HashMap();
  private Map extracts = new HashMap();

  private Properties properties = null;

  private boolean parsed = false;

  public ExtractConfigurationParser() {}

  public ExtractConfigurationParser(Properties properties) {
    setConfiguration(properties);
  }
  
  public void setConfiguration(Properties properties) {
    this.properties = properties;
  }

  public boolean parseConfiguration() {
    if (properties == null)
      return false;

    List orphans = getOrphanExtracts();
    Map tmpExtracts = new HashMap();
    for (int i = 0; i < orphans.size(); i++) {
      try {
        tmpExtracts.put(orphans.get(i), getExtractConfig((String) orphans.get(i), i));
      } catch (Exception ex) {
        log.error("Unable to parse information for extracts " + orphans.get(i));
        log.error("Message: " + ex.getMessage());
        log.error(ex);
        return false;
      }
    }
    extracts = tmpExtracts;

    String batchNameString = properties.getProperty(MODULE_NAME + "." + BATCH_NAMES);
    String[] batchNames = (batchNameString != null ? batchNameString.split(NAMES_SEPARATOR) : new String[0]);
    Map tmpBatches = new HashMap();
    for (int i = 0; i < batchNames.length; i++) {
      try {
        tmpBatches.put(batchNames[i], parseForBatch(batchNames[i]));
      } catch (Exception ex) {
        log.error("Unable to parse information for data extractor batch "
            + batchNames[i]);
        log.error("Message: " + ex.getMessage());
        log.error(ex);
        return false;
      }
    }
    batchExtract = tmpBatches;
    parsed = true;
    return parsed;
  }
  
  private Map parseForBatch(String batchName) {
    Map loadConfig = new HashMap();
    String loadNameString = properties.getProperty(MODULE_NAME + "." + batchName + "." + EXTRACT_NAMES, "");
    String hourToRun = properties.getProperty(MODULE_NAME + "." + batchName + "." + START_HOUR, "");
    String minuteToRun = properties.getProperty(MODULE_NAME + "." + batchName + "." + START_MINUTE, "");
    String[] loadNames = loadNameString.split(NAMES_SEPARATOR);
    try {
      int hour = Integer.parseInt(hourToRun);
      int minute = Integer.parseInt(minuteToRun);
      Calendar cal = Calendar.getInstance();
      cal.set(Calendar.HOUR_OF_DAY, hour);
      cal.set(Calendar.MINUTE, minute);
      loadConfig.put(START_TIME, cal.getTime());
    } catch (Exception e) {
      throw new RuntimeException("Invalid start time to parse for batch " + batchName + "- " + hourToRun + ":" + minuteToRun, e);
    }
    Map comparatorConfig = new HashMap();
    List tmpConfigs = new ArrayList();
    for (int i = 0; i < loadNames.length; i++) {
      if (extracts.get(loadNames[i]) == null)
        continue;
      Map tmpConfig = getExtractConfig(loadNames[i], i); 
      tmpConfigs.add(tmpConfig);
      comparatorConfig.put(tmpConfig, (Integer) tmpConfig.get(SEQUENCE));
    }
    Comparator mapComparator = new MapComparator(comparatorConfig);
    Arrays.sort(tmpConfigs.toArray(new Map[0]), mapComparator);
    String[] sortedLoadNames = new String[tmpConfigs.size()];
    for (int i = 0; i < tmpConfigs.size(); i++)
      sortedLoadNames[i] = (String)((Map)tmpConfigs.get(i)).get(NAME);
    loadConfig.put(EXTRACT_NAMES, sortedLoadNames);
    return loadConfig;
  }
  
  private Map getExtractConfig(String loadName, int seq){
    Map tmpConfig = new HashMap();
    Integer sequence = new Integer(properties.getProperty(MODULE_NAME + "." + loadName + "." + SEQUENCE, "" + seq));
    tmpConfig.put(SEQUENCE, sequence);
    tmpConfig.put(NAME, loadName);
    Integer skipSize = new Integer(properties.getProperty(MODULE_NAME + "." + loadName + "." + SKIP, "0"));
    tmpConfig.put(SKIP, skipSize);
    Integer fetchSize = new Integer( properties.getProperty(MODULE_NAME + "." + loadName + "." + FETCH, "1"));
    tmpConfig.put(FETCH, fetchSize);
    tmpConfig.put(FILE_LOCATION, properties.getProperty(MODULE_NAME + "."
        + loadName + "." + FILE_LOCATION, "NA"));
    tmpConfig.put(FILE_NAME, properties.getProperty(MODULE_NAME + "."
        + loadName + "." + FILE_NAME, "NA"));
    SimpleDateFormat format = new SimpleDateFormat(properties.getProperty(MODULE_NAME + "." + loadName + "." + FILE_NAME_DATE_FORMAT, "yyyyMMdd"));
    tmpConfig.put(FILE_NAME_DATE_FORMAT, format);
    tmpConfig.put(CONFIG_FILE, properties.getProperty(MODULE_NAME + "."
        + loadName + "." + CONFIG_FILE, ""));
    tmpConfig.put(DEPENDS, properties.getProperty(MODULE_NAME + "."
        + loadName + "." + DEPENDS));
    Long waitForFile = new Long(properties.getProperty(MODULE_NAME + "."+ loadName + "." + WAIT_FOR_FILE, "0"));
    tmpConfig.put(WAIT_FOR_FILE, waitForFile);
    tmpConfig.put(WAIT_FILE_EXT, properties.getProperty(MODULE_NAME + "."+ loadName + "." + WAIT_FILE_EXT, ".control"));
    tmpConfig.put(DATA_SOURCE, properties.getProperty(MODULE_NAME + "."+ loadName + "." + DATA_SOURCE, null));
    return tmpConfig;
  }
  
  public boolean parseConfiguration(Properties properties) {
    if (properties == null)
      return false;
    this.properties = properties;
    return parseConfiguration();
  }
  
  public List getBatchNames() throws NotParsedException {
    Map configs = getConfigs();
    List batchNames = new ArrayList();
    batchNames.addAll(configs.keySet());
    return batchNames;
  }

  public List getExtractNames() throws NotParsedException {
    Map configs = getExtractConfigs();
    List extractNames = new ArrayList();
    extractNames.addAll(configs.keySet());
    return extractNames;
  }
  
  public boolean extractExist(String loadName) throws NotParsedException{
    return getExtractConfigs().get(loadName) != null;
  }
  
  public boolean batchExist(String batchName) throws NotParsedException{
    return getConfigs().get(batchName) != null;
  }
  
  public List getSortedExtractNames() throws NotParsedException {
    Map configs = getExtractConfigs();
    Map comparatorConfig = new HashMap();
    Map[] sortable = (HashMap[])configs.values().toArray(new HashMap[0]);
    for (int i = 0; i < sortable.length; i++)
      comparatorConfig.put(sortable[i], (Integer) sortable[i].get(SEQUENCE));
    Arrays.sort(sortable, new MapComparator(comparatorConfig));
    List sortedExtractNames = new ArrayList();
    for (int i = 0; i < sortable.length; i++)
      sortedExtractNames.add(sortable[i].get(NAME));
    return sortedExtractNames;
  }

  public Date getStartTimeForBatch(String batchName) throws NotParsedException, BatchNotExistException {
    checkParsed();
    return (Date)getBatchConfig(batchName).get(START_TIME);
  }
  
  public List getExtractNamesForBatch(String batchName) throws NotParsedException, BatchNotExistException {
    checkParsed();
    String[] extractNames = (String[]) getBatchConfig(batchName).get(EXTRACT_NAMES);
    List extractNameList = new ArrayList();
    for (int i = 0; i < extractNames.length; i++)
      extractNameList.add(extractNames[i]);
    return extractNameList;
  }
  
  public Map getBatchConfig(String batchName) throws NotParsedException, BatchNotExistException {
    checkParsed();
    Map batchConfig = (Map)batchExtract.get(batchName);
    if (batchConfig == null)
      throw new BatchNotExistException("No batch with the name " + batchName + " is configured.");
    return batchConfig;
  }

  public Date getBatchStartTime(String batchName) throws NotParsedException, BatchNotExistException {
    return (Date)getBatchConfig(batchName).get(START_TIME);
  }
  
  public Map getConfigs() throws NotParsedException {
    checkParsed();
    return batchExtract;
  }

  public Map getExtractConfigs() throws NotParsedException {
    checkParsed();
    return extracts;
  }
  
  public void checkParsed() throws NotParsedException{
    if (!isParsed())
      throw new NotParsedException();
  }

  public Map getConfigsForExtract(String name) throws NotParsedException, LoadNotExistException{
    checkParsed();
    if (extracts.get(name) != null)
      return (Map)extracts.get(name);
    else
      throw new LoadNotExistException("No load with the name " + name + " is configured.");
  }
  
  public boolean isParsed() {
    return parsed;
  }

  public Object clone(){
    Properties newProperties = new Properties();
    newProperties.putAll(this.properties);
    ExtractConfigurationParser newParser = new ExtractConfigurationParser(newProperties);
    newParser.parseConfiguration();
    return newParser;
  }
  
  private List getOrphanExtracts(){
    List orphans = new ArrayList();
    Enumeration enumer = properties.propertyNames();
    while (enumer.hasMoreElements()){
      String tmpProp = (String)enumer.nextElement();
        if (tmpProp.indexOf(ExtractConfigurationParser.SEQUENCE) >= 0 && tmpProp.startsWith(ExtractConfigurationParser.MODULE_NAME)){
          String tmpString = tmpProp.substring(tmpProp.indexOf(".") + 1, tmpProp.lastIndexOf("."));
          orphans.add(tmpString);
        }
    }
    return orphans;
  }

  public static void main(String args[]) throws NotParsedException, LoadNotExistException, BatchNotExistException, Exception {
    Properties props = null;
    if (args.length != 1 || args[0].trim().length() == 0) {
      System.out
          .println("Syntax: java com.profitera.dc.parser.ExtractConfigurationParser <configuration file>");
      System.exit(1);
    }
    System.out.println("Loading configuration..");
    try {
      props = Utilities.load(args[0]);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    if (props == null)
      try {
        props = Utilities.loadFromPath(args[0]);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    if (props == null) {
      System.out.println("Unable to extract configuration file " + args[0]);
      System.exit(1);
    }
    System.out.println("Initializing parser..");
    ExtractConfigurationParser ecp = new ExtractConfigurationParser(props);
    System.out.println("Parsing configuration..");
    ecp.parseConfiguration();
    if (!ecp.isParsed()) {
      System.out
          .println("Problem occured while trying to parse configuration file "
              + args[0]);
      System.exit(1);
    }
    System.out.println("Displaying configuration..");
    ExtractConfigurationParser.printConfigurations(ecp);
    System.out.println("\nEnd.");
  }
  
  public static void printExtractConfigurations(ExtractConfigurationParser ecp) throws NotParsedException, LoadNotExistException{
    System.out.println("Displaying load configurations..");
    List sortedExtracts = ecp.getSortedExtractNames();
    for (int i = 0; i < sortedExtracts.size(); i++){
      Map tmpMap = ecp.getConfigsForExtract((String)sortedExtracts.get(i));
      System.out.println("---------------------------------------");
      System.out.println("Load name: "
          + tmpMap.get(ExtractConfigurationParser.NAME));
      System.out.println("Load sequence: "
          + tmpMap.get(ExtractConfigurationParser.SEQUENCE));
      System.out.println("Fetch size: "
          + tmpMap.get(ExtractConfigurationParser.FETCH));
      System.out.println("Skip Size: "
          + tmpMap.get(ExtractConfigurationParser.SKIP));
      System.out.println("File Location: "
          + tmpMap.get(ExtractConfigurationParser.FILE_LOCATION));
      System.out.println("File Name: "
          + tmpMap.get(ExtractConfigurationParser.FILE_NAME));
      System.out.println("File Name Date Format: "
          + ((SimpleDateFormat)tmpMap.get(ExtractConfigurationParser.FILE_NAME_DATE_FORMAT)).toPattern());
      System.out.println("Config File: "
          + tmpMap.get(ExtractConfigurationParser.CONFIG_FILE));
      System.out.println("Depends On: "
          + tmpMap.get(ExtractConfigurationParser.DEPENDS));
    }
  }
  
  public static void printBatchConfigurations(ExtractConfigurationParser ecp) throws NotParsedException, BatchNotExistException{
    System.out.println("Displaying batch configurations..");
    List batches = ecp.getBatchNames();
    for (int i = 0; i < batches.size(); i++){
      String batchName = (String)batches.get(i);
      Map batchCfg = ecp.getBatchConfig(batchName);
      System.out.println("Batch Name:\t" + batchName);
      System.out.println("Start Time:\t" + batchCfg.get(ExtractConfigurationParser.START_TIME));
      String[] configuredLoads = (String[])batchCfg.get(ExtractConfigurationParser.EXTRACT_NAMES);
      System.out.println("Configured Extracts:");
      for (int j = 0; j < configuredLoads.length; j++)
        System.out.println("\t\t" + (j + 1) + ". " + configuredLoads[j]);
    }  	
  }
  
  public static void printConfigurations(ExtractConfigurationParser ecp) throws NotParsedException, LoadNotExistException, BatchNotExistException {
  	printExtractConfigurations(ecp);
  	printBatchConfigurations(ecp);
  }
}