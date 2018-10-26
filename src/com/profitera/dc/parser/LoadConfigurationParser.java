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

import com.profitera.dc.parser.exception.BatchNotExistException;
import com.profitera.dc.parser.exception.LoadNotExistException;
import com.profitera.dc.parser.exception.NotParsedException;
import com.profitera.util.MapComparator;
import com.profitera.util.Utilities;

public class LoadConfigurationParser {
  public enum OutputType {JDBC, FIXED_WIDTH_TEXT, NATIVE};
  
  private static final String NAMES_SEPARATOR = ";";

  public static final String MODULE_NAME = "dataloader";

  public static final String BATCH_NAMES = "batchNames";

  public static final String LOAD_NAMES = "loadNames";
  public static final String START_TIME = "startTime";
  public static final String START_HOUR = "startHour";
  public static final String START_MINUTE = "startMinute";
  
  public static final String SEQUENCE = "sequence";
  public static final String NAME = "name";
  public static final String THREADS = "threads";
  public static final String COMMIT_SIZE = "commitSize";
  public static final String FILE_LOCATION = "fileLocation";
  public static final String FILE_NAME = "fileName";
  public static final String FILE_NAME_DATE_FORMAT = "fileNameDateFormat";
  public static final String CONFIG_FILE = "configFile";
  public static final String DISABLE_FILTER_FILE = "disableFilterFile";
  public static final String DEPENDS = "depends";
  public static final String WAIT_FOR_FILE = "waitForFile";
  public static final String WAIT_FILE_EXT = "waitFileExt";
  public static final String WAIT_FILE_REPLACE = "waitFileReplace";
  public static final String WAIT_FILE_PATH = "waitFilePath";
  public static final String DATA_SOURCE = "dataSource";
  public static final String OUTPUT = "output";
  public static final String OUTPUT_NATIVE_COPY_CMD = "outputCopyToDatabaseServerCommand";
  public static final String OUTPUT_NATIVE_SERVER_PATH = "outputDatabaseServerPath";
  public static final String OUTPUT_NATIVE_RECREATE = "outputRecreate";
  public static final String DIFF = "diff";
  public static final String SORT = "allowDiffSort";
  
  public static final String OUTPUT_FILE_NAME = "outputFileName";

  public static final String DC_STACKTRACE_DIR = "DataConnectorStacktraceDir";
  public static final String DC_BADFILE_DIR = "DataConnectorBadFileDir";
  public static final String DC_FILTER_DIR = "DataConnectorFilterFileDir";

  public static final String[] BATCH_PROPERTIES = {LOAD_NAMES, START_TIME};
  public static final Object[] BATCH_PROPERTIES_TYPE = {String.class, Date.class};

  private Map<String, Map<String, Object>> batchLoads = new HashMap<String, Map<String, Object>>();
  private Map<String, Map<String, Object>> loads = new HashMap<String, Map<String, Object>>();

  private final Properties properties;

  public LoadConfigurationParser(Properties properties) {
    if (properties == null) {
      properties = new Properties();
    }
    this.properties = properties;
    parseConfiguration();
  }

  private void parseConfiguration() {
    List<String> orphans = getOrphanLoads();
    Map<String, Map<String, Object>> tmpLoads = new HashMap<String, Map<String, Object>>();
    for (int i = 0; i < orphans.size(); i++) {
      try {
        tmpLoads.put(orphans.get(i), getLoadConfig((String) orphans.get(i), i));
      } catch (Exception ex) {
        throw new RuntimeException("Unable to parse information for load " + orphans.get(i), ex);
      }
    }
    loads = tmpLoads;

    String batchNameString = properties.getProperty(MODULE_NAME + "." + BATCH_NAMES);
    String[] batchNames = (batchNameString != null ? batchNameString.split(NAMES_SEPARATOR) : new String[0]);
    Map<String, Map<String, Object>> tmpBatches = new HashMap<String, Map<String, Object>>();
    for (int i = 0; i < batchNames.length; i++) {
      try {
        tmpBatches.put(batchNames[i], parseForBatch(batchNames[i]));
      } catch (Exception ex) {
        throw new RuntimeException("Unable to parse information for data loader batch "
            + batchNames[i], ex);
      }
    }
    batchLoads = tmpBatches;
  }
  
  private Map<String, Object> parseForBatch(String batchName) {
    Map<String, Object> loadConfig = new HashMap<String, Object>();
    String loadNameString = properties.getProperty(MODULE_NAME + "." + batchName + "." + LOAD_NAMES, "").trim();
    String hourToRun = properties.getProperty(MODULE_NAME + "." + batchName + "." + START_HOUR, "").trim();
    String minuteToRun = properties.getProperty(MODULE_NAME + "." + batchName + "." + START_MINUTE, "").trim();
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
    Map<Map<String, Object>, Integer> comparatorConfig = new HashMap<>();
    List<Map<String, Object>> tmpConfigs = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < loadNames.length; i++) {
      if (loads.get(loadNames[i]) == null)
        continue;
      Map<String, Object> tmpConfig = getLoadConfig(loadNames[i], i); 
      tmpConfigs.add(tmpConfig);
      comparatorConfig.put(tmpConfig, (Integer) tmpConfig.get(SEQUENCE));
    }
    Comparator mapComparator = new MapComparator(comparatorConfig);
    Arrays.sort(tmpConfigs.toArray(new Map[0]), mapComparator);
    String[] sortedLoadNames = new String[tmpConfigs.size()];
    for (int i = 0; i < tmpConfigs.size(); i++)
      sortedLoadNames[i] = (String)tmpConfigs.get(i).get(NAME);
    loadConfig.put(LOAD_NAMES, sortedLoadNames);
    return loadConfig;
  }
  
  private Map<String, Object> getLoadConfig(String loadName, int seq){
    Map<String, Object> tmpConfig = new HashMap<String, Object>();
    Integer sequence = new Integer(properties.getProperty(MODULE_NAME + "." + loadName + "." + SEQUENCE, "" + seq).trim());
    tmpConfig.put(SEQUENCE, sequence);
    tmpConfig.put(NAME, loadName);
    Integer threads = new Integer(properties.getProperty(MODULE_NAME + "." + loadName + "." + THREADS, "1").trim());
    tmpConfig.put(THREADS, threads);
    Integer commitSize = new Integer( properties.getProperty(MODULE_NAME + "." + loadName + "." + COMMIT_SIZE, "1").trim());
    tmpConfig.put(COMMIT_SIZE, commitSize);
    assign(tmpConfig, loadName, FILE_LOCATION, "NA", true);
    assign(tmpConfig, loadName, FILE_NAME, "NA", true);
    SimpleDateFormat format = new SimpleDateFormat(properties.getProperty(MODULE_NAME + "." + loadName + "." + FILE_NAME_DATE_FORMAT, "yyyyMMdd").trim());
    tmpConfig.put(FILE_NAME_DATE_FORMAT, format);
    assign(tmpConfig, loadName, CONFIG_FILE, "NA", true);
    assign(tmpConfig, loadName, DEPENDS, null, false);
    Long waitForFile = new Long(properties.getProperty(MODULE_NAME + "."+ loadName + "." + WAIT_FOR_FILE, "0").trim());
    tmpConfig.put(WAIT_FOR_FILE, waitForFile);
    assign(tmpConfig, loadName, WAIT_FILE_EXT, ".control", true);
    assign(tmpConfig, loadName, WAIT_FILE_REPLACE, "$", true);
    assign(tmpConfig, loadName, WAIT_FILE_PATH, null, true);
    String disbaleFilterFile = properties.getProperty(MODULE_NAME + "."+ loadName + "." + DISABLE_FILTER_FILE, "false").trim();
    tmpConfig.put(DISABLE_FILTER_FILE, disbaleFilterFile.toLowerCase().startsWith("t") ? new Boolean(true): new Boolean(false));
    assign(tmpConfig, loadName, DATA_SOURCE, null, false);
    assign(tmpConfig, loadName, OUTPUT, null, false);
    assign(tmpConfig, loadName, OUTPUT_NATIVE_COPY_CMD, null, false);
    assign(tmpConfig, loadName, OUTPUT_NATIVE_SERVER_PATH, null, false);
    assign(tmpConfig, loadName, OUTPUT_NATIVE_RECREATE, "true", false);
    return tmpConfig;
  }
  
  private void assign(Map<String, Object> p, String loadName, String prop, String defaultValue, boolean doTrim) {
    String propertyValue = properties.getProperty(MODULE_NAME + "."+ loadName + "." + prop, defaultValue);
    if (doTrim && propertyValue != null) {
      p.put(prop, propertyValue.trim());
    } else {
      p.put(prop, propertyValue);
    }
  }

  public List<String> getBatchNames() throws NotParsedException {
    Map<String, Map<String, Object>> configs = getConfigs();
    List<String> batchNames = new ArrayList<String>();
    batchNames.addAll(configs.keySet());
    return batchNames;
  }

  public List<String> getLoadNames() throws NotParsedException {
    Map<String, Map<String, Object>> configs = getLoadConfigs();
    List<String> loadNames = new ArrayList<String>();
    loadNames.addAll(configs.keySet());
    return loadNames;
  }
  
  public boolean loadExist(String loadName) throws NotParsedException{
    return getLoadConfigs().get(loadName) != null;
  }
  
  public boolean batchExist(String batchName) throws NotParsedException{
    return getConfigs().get(batchName) != null;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public List<String> getSortedLoadNames() throws NotParsedException {
    Map<Map<Object, Object>, Integer> comparatorConfig = new HashMap<Map<Object, Object>, Integer>();
    Map[] sortable = (Map[])getLoadConfigs().values().toArray(new HashMap[0]);
    for (int i = 0; i < sortable.length; i++) {
      comparatorConfig.put(sortable[i], (Integer) sortable[i].get(SEQUENCE));
    }
    Arrays.sort(sortable, new MapComparator(comparatorConfig));
    List<String> sortedLoadNames = new ArrayList<String>();
    for (int i = 0; i < sortable.length; i++) {
      sortedLoadNames.add((String) sortable[i].get(NAME));
    }
    return sortedLoadNames;
  }

  public List<String> getLoadNamesForBatch(String batchName) throws NotParsedException, BatchNotExistException {
    String[] loadNames = (String[]) getBatchConfig(batchName).get(LOAD_NAMES);
    List<String> loadNameList = new ArrayList<String>();
    for (int i = 0; i < loadNames.length; i++)
      loadNameList.add(loadNames[i]);
    return loadNameList;
  }
  
  public Map<String, Object> getBatchConfig(String batchName) throws NotParsedException, BatchNotExistException {
    Map<String, Object> batchConfig = batchLoads.get(batchName);
    if (batchConfig == null)
      throw new BatchNotExistException("No batch with the name " + batchName + " is configured.");
    return batchConfig;
  }

  public Date getBatchStartTime(String batchName) throws NotParsedException, BatchNotExistException {
    return (Date)getBatchConfig(batchName).get(START_TIME);
  }
  
  public Map<String, Map<String, Object>> getConfigs() throws NotParsedException {
    return batchLoads;
  }

  public Map<String, Map<String, Object>> getLoadConfigs() {
    return loads;
  }
  public Map<String, Object> getConfigsForLoad(String name) throws LoadNotExistException{
    if (loads.get(name) != null)
      return loads.get(name);
    else
      throw new LoadNotExistException("No load with the name " + name + " is configured.");
  }

  public Object clone(){
    Properties newProperties = new Properties();
    newProperties.putAll(this.properties);
    LoadConfigurationParser newParser = new LoadConfigurationParser(newProperties);
    newParser.parseConfiguration();
    return newParser;
  }
  
  private List<String> getOrphanLoads(){
    List<String> orphans = new ArrayList<String>();
    Enumeration<?> enumer = properties.propertyNames();
    while (enumer.hasMoreElements()){
      String tmpProp = (String)enumer.nextElement();
        if (tmpProp.endsWith("." + LoadConfigurationParser.SEQUENCE)){
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
          .println("Syntax: java com.profitera.dc.parser.LoadConfigurationParser <configuration file>");
      System.exit(1);
    }
    System.out.println("Loading configuration..");
    try {
      props = Utilities.load(args[0]);
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (props == null) {
      props = Utilities.loadFromPath(args[0]);
    }
    if (props == null) {
      System.out.println("Unable to load configuration file " + args[0]);
      System.exit(1);
    }
    System.out.println("Initializing parser..");
    LoadConfigurationParser lcp = new LoadConfigurationParser(props);
    System.out.println("Parsing configuration..");
    lcp.parseConfiguration();
    System.out.println("Displaying configuration..");
    LoadConfigurationParser.printConfigurations(lcp);
    System.out.println("\nEnd.");
  }
  
  public static void printLoadConfigurations(LoadConfigurationParser lcp) throws NotParsedException, LoadNotExistException{
    System.out.println("Displaying load configurations..");
    List<String> sortedLoads = lcp.getSortedLoadNames();
    for (int i = 0; i < sortedLoads.size(); i++){
      Map<String, Object> tmpMap = lcp.getConfigsForLoad((String)sortedLoads.get(i));
      System.out.println("---------------------------------------");
      System.out.println("Load name: "
          + tmpMap.get(LoadConfigurationParser.NAME));
      System.out.println("Load sequence: "
          + tmpMap.get(LoadConfigurationParser.SEQUENCE));
      System.out.println("No. of Threads: "
          + tmpMap.get(LoadConfigurationParser.THREADS));
      System.out.println("Commit Size: "
          + tmpMap.get(LoadConfigurationParser.COMMIT_SIZE));
      System.out.println("File Location: "
          + tmpMap.get(LoadConfigurationParser.FILE_LOCATION));
      System.out.println("File Name: "
          + tmpMap.get(LoadConfigurationParser.FILE_NAME));
      System.out.println("File Name Date Format: "
          + ((SimpleDateFormat)tmpMap.get(LoadConfigurationParser.FILE_NAME_DATE_FORMAT)).toPattern());
      System.out.println("Config File: "
          + tmpMap.get(LoadConfigurationParser.CONFIG_FILE));
      System.out.println("Depends On: "
          + tmpMap.get(LoadConfigurationParser.DEPENDS));
    }
  }
  
  public static void printBatchConfigurations(LoadConfigurationParser lcp) throws NotParsedException, BatchNotExistException{
    System.out.println("Displaying batch configurations..");
    List<String> batches = lcp.getBatchNames();
    for (int i = 0; i < batches.size(); i++){
      String batchName = (String)batches.get(i);
      Map<String, Object> batchCfg = lcp.getBatchConfig(batchName);
      System.out.println("Batch Name:\t" + batchName);
      System.out.println("Start Time:\t" + batchCfg.get(LoadConfigurationParser.START_TIME));
      String[] configuredLoads = (String[])batchCfg.get(LoadConfigurationParser.LOAD_NAMES);
      System.out.println("Configured Loads:");
      for (int j = 0; j < configuredLoads.length; j++)
        System.out.println("\t\t" + (j + 1) + ". " + configuredLoads[j]);
    }  	
  }
  
  public static void printConfigurations(LoadConfigurationParser lcp) throws NotParsedException, LoadNotExistException, BatchNotExistException {
  	printLoadConfigurations(lcp);
  	printBatchConfigurations(lcp);
  }

  public OutputType getLoadOutput(String loadName) throws NotParsedException {
    Map<String, Object> conf = getLoadConfiguration(loadName);
    if (conf.get(OUTPUT) == null || conf.get(OUTPUT).equals("db")) {
      return OutputType.JDBC;
    } else if (conf.get(OUTPUT).equals("fixedwidthfile")) {
      return OutputType.FIXED_WIDTH_TEXT;
    } else if (conf.get(OUTPUT).equals("nativeloader")) {
      return OutputType.NATIVE;
    } else {
      throw new IllegalStateException("Unsupported output type for load " + loadName);
    }
  }

  private Map<String, Object> getLoadConfiguration(String loadName) throws NotParsedException {
    Map<String, Object> conf = getLoadConfigs().get(loadName);
    return conf;
  }

  public String getLoadNativeLoaderCopyCommand(String name) throws NotParsedException {
    return (String) getLoadConfiguration(name).get(OUTPUT_NATIVE_COPY_CMD);
  }
  public String getLoadNativeLoaderServerPath(String name) throws NotParsedException {
    return (String) getLoadConfiguration(name).get(OUTPUT_NATIVE_SERVER_PATH);
  }

  public String getLogDirectory() {
    return properties.getProperty("dataloader.logDir");
  }

  public boolean isLoadNativeLoaderRecreate(String name) throws NotParsedException {
    String s = (String) getLoadConfiguration(name).get(OUTPUT_NATIVE_RECREATE);
    return s.endsWith("true");
  }

  public int getCommitSize(String loadName) {
    try {
      return ((Integer)getLoadConfiguration(loadName).get(LoadConfigurationParser.COMMIT_SIZE)).intValue();
    } catch (NotParsedException e) {
      throw new RuntimeException(e);
    }
  }

  public int getThreadCount(String loadName) {
    try {
      return ((Integer)getLoadConfiguration(loadName).get(LoadConfigurationParser.THREADS)).intValue();
    } catch (NotParsedException e) {
      throw new RuntimeException(e);
    }
  }
  public String getFileOutputPath(String loadName) {
    return getLoadProperty(loadName, "outputLocalPath");
  }

  private String getLoadProperty(String loadName, String property) {
    return properties.getProperty(MODULE_NAME + "." + loadName + "." + property);
  }
}