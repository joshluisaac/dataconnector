package com.profitera.dc.parser;

public interface PropertiesConstantKey {
  public static final String LOAD_TYPE_KEY = "loadType";
  public static final String DELIMITER = "delimiter";
  public static final String SELECT_QUERY_WHERE_KEY = "selectQueryWhere";
  public static final String UPDATE_QUERY_WHERE_KEY = "updateQueryWhere";
  public static final String POST_INSERT_UPDATE_KEY = "postInsertUpdate";
  public static final String POST_INSERT_INSERTION_KEY = "postInsertInsertion";
  public static final String POST_UPDATE_UPDATE_KEY = "postUpdateUpdate";
  public static final String POST_UPDATE_INSERTION_KEY = "postUpdateInsertion";
  public static final String IS_OPTIONAL_SUFFIX = "_isOptional";
  public static final String IS_EXTERNAL_SUFFIX = "_isExternal";
  public static final String DEFAULT_SUFFIX = "_default";
  public static final String LOOKUP_QUERY_SUFFIX = "_lookupQuery";
  public static final String LOOKUP_DATA_TYPE = "_lookupType";
  public static final String LOOKUP_INSERT_SUFFIX = "_lookupInsert";
  public static final String LOOKUP_KEY_GEN_SUFFIX = "_lookupInsertKey";
  public static final String IS_KEY_SUFFIX = "_isKey";
  public static final String LOCATION_SUFFIX = "_location";
  public static final String HANDLER_SUFFIX = "_handler";
  public static final String HANDLER_ARGS_SUFFIX = "_handlerConfig";
  public static final String FILTER_ON_SUFFIX = "_filterOn";
  public static final String NULL_DEFINITION_SUFFIX = "_nullDefinition";
  public static final String CACHE = "_cache";
  public static final String GENERATED_KEY_KEY = "generatedKey";
  public static final String GENERATED_KEY_SEQ_NAME_KEY = "generatedKeySeqName";
  public static final String TABLE_KEY = "table";
  public static final String PAD_LINE_KEY = "padLine";
  public static final String REFRESH_DATA_KEY = "refreshData";
  public static final String REFRESH_DATA_QUERY = "refreshDataQuery";
  public static final String REFRESH_DATA_QUERY_TIMEOUT = "refreshDataQueryTimeout"; 
  public static final String HEADER_NAME_KEY = "headerName";
  public static final String TRAILER_NAME_KEY = "trailerName";
	public static final String XML_START_TAG = "recordIndicatorTag";
	public static final String XQUERY = "xQuery";
}
