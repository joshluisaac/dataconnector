package com.profitera.dc.lookup;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.ibatis.sqlmap.client.event.RowHandler;
import com.profitera.dc.ILoadingProcess;
import com.profitera.dc.InvalidExecutionThreadException;
import com.profitera.dc.InvalidLineException;
import com.profitera.dc.InvalidLookupQueryException;
import com.profitera.dc.MissingLookupException;
import com.profitera.dc.lookup.impl.DataLoaderLookupLogClient;
import com.profitera.dc.parser.LoadingQueryWriter;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.LookupDefinition;
import com.profitera.log.ILogProvider;

public class DefaultLookupCache implements ILookupCache {
  private String field;
  private SqlMapClient reader;
  private SqlMapClient writer;
  private final ILogProvider log;
  private ParameterCache cache;
  private boolean isOptional = false;
  private boolean isCachingEnabled = true;
  private String lookupQueryName;
  private boolean isFullCache = false;
  private String[] lookupQueryParams;
  private boolean hasLookupInsert;
  private String lookupInsertName;
  private int maximumCacheSize = -1;
  private final String destinationTable;
  public DefaultLookupCache(String destinationTable, ILogProvider p) {
    this.log = p;
    this.destinationTable = destinationTable;
  }
  public void configure(String name, LookupDefinition ld, SqlMapClient reader, SqlMapClient writer) {
    if (ld == null) {
      throw new IllegalArgumentException("No lookup definition provided");
    }
    if (reader == null) {
      throw new IllegalArgumentException("No reader provided");
    }
    if (writer == null) {
      throw new IllegalArgumentException("No writer provided");
    }
    this.reader = reader;
    this.writer = writer;
    this.field = name;
    isOptional = ld.isLookupOptional();
    isCachingEnabled = ld.isLookupCache();
    lookupQueryName = LoadingQueryWriter.getLookupQueryName(field);
    List<String> temp = new ArrayList<String>();
    try {
      temp = Arrays.asList(ld.getLookupQueryParams());
    } catch (InvalidLookupQueryException e) {
      // should never hit this if the definition was validated
      getLog().emit(DataLoaderLookupLogClient.PARAM_ERROR, field, e);
      throw new RuntimeException("Failed to retrieve lookup parameters for field " + field, e);
    }
    lookupQueryParams = new String[temp.size()];
    for (int i = 0; i < lookupQueryParams.length; i++) {
      lookupQueryParams[i] = (String) temp.get(i);
    }
    hasLookupInsert = ld.hasLookupInsert();
    if (hasLookupInsert) {
      lookupInsertName = LoadingQueryWriter.getLookupInsertName(field);
    }
    if (ld.getLookupFullCacheQuery() == null) {
      // Only non-full caches can have caps
      cache = new ParameterCache(lookupQueryParams, 1000, ld.getMaximumCacheSize());
    } else {
      cache = new ParameterCache(lookupQueryParams, 1000);
      final String fullCacheResultName = LoadingQueryWriter.getLookupFullCacheResultName(field);
      final String fullCacheQueryName = LoadingQueryWriter.getLookupFullCacheQueryName(field);
      getLog().emit(DataLoaderLookupLogClient.EXEC_FULL_CACHE, field);
      try {
        reader.queryWithRowHandler(fullCacheQueryName, new RowHandler<Map<String, Object>>() {
          public void handleRow(Map<String, Object> m) {
            Object value = m.get(fullCacheResultName);
            cache.storeInCache(m, value);
          }
        });
        isFullCache = true;
      } catch (SQLException e) {
        getLog().emit(DataLoaderLookupLogClient.EXEC_FULL_CACHE_ERROR, field, e);
        throw new RuntimeException("Lookup cache failed for " + field + ", reverts to normal lookup", e);
      }
      getLog().emit(DataLoaderLookupLogClient.EXEC_FULL_CACHE_DONE, field);
    }
  }

  private ILogProvider getLog() {
    return log;
  }

  @Override
  public Object getLookupValue(Map<String, Object> params, long line, ILoadingProcess proc)
      throws InvalidExecutionThreadException {
    if (field == null) {
      throw new IllegalStateException("Cache is not yet configured");
    }
    if (hasLookupInsert) {
      proc.acquireSemaphorePermit();
    }
    try {
      String lookupCond = "";
      String[] lookupConds = lookupQueryParams;
      for (int i = 0; i < lookupConds.length; i++) {
        lookupCond += "" + lookupConds[i] + "=" + params.get(lookupConds[i]) + ";";
      }
      Object lookedUp = null;
      if (isCachingEnabled) {
        lookedUp = cache.getFromCache(params);
      }
      if (lookedUp == ParameterCache.MASK_NULL) {
        getLog().emit(DataLoaderLookupLogClient.CACHE_RESOLVED_NULL, field, lookupCond);
        return null;
      }
      if (lookedUp != null) {
        getLog().emit(DataLoaderLookupLogClient.CACHE_RESOLVED, field, lookupCond, lookedUp);
        return lookedUp;
      }
      try {
        if (!isFullCache) {
          // Inform the executing thread that there is a db call being made
          proc.startDbCall(lookupQueryName);
          // Even if this lookup can insert, we can query it from the
          // read-only connection because the cache resolves them for us
          // so that we don't have issues... the synchronization is managed
          // inside the get through insert.
          lookedUp = reader.queryForObject(lookupQueryName, params);
          // Inform the executing thread that the db call has been completed
          proc.endDbCall();
          if (!proc.isValid()) {
            throw buildInvalidExecution(params);
          }
        }
      } catch (SQLException e1) {
        // Inform the executing thread that the db call has been completed, even
        // if it caused an error
        proc.endDbCall();
        if (!proc.isValid()) {
          throw buildInvalidExecution(params);
        }
        throw new InvalidLineException("Lookup query for " + field + " invalid: " + lookupQueryName, e1, line);
      }
      try {
        if (lookedUp == null && hasLookupInsert) {
          if (isAllParametersNull(params)) {
            getLog().emit(DataLoaderLookupLogClient.NO_INSERT, field, lookupCond);
          } else {
            // This also adds the value to the cache in a sychronized manner if
            // found
            lookedUp = getLookupThroughInsert(writer, params, proc);
          }
        }
      } catch (SQLException e1) {
        throw new InvalidLineException("Lookup insert for " + field + " failed: " + lookupInsertName, e1, line);
      }
      if (lookedUp == null) {
        if (!isOptional) {
          throw new MissingLookupException(getDestinationTable(), lookupCond, params.get(field), line);
        } else {
          getLog().emit(DataLoaderLookupLogClient.NOT_RESOLVED, field, lookupCond);
        }
      } else {
        getLog().emit(DataLoaderLookupLogClient.RESOLVED, field, lookupCond, lookedUp);
      }
      if (isCachingEnabled) {
        cache.storeInCache(params, lookedUp);
      }
      return lookedUp;
    } finally {
      if (hasLookupInsert) {
        proc.releaseSemaphorePermit();
      }
    }
  }
  private String getDestinationTable() {
    return destinationTable;
  }
  private InvalidExecutionThreadException buildInvalidExecution(Map<String, Object> params) {
    return new InvalidExecutionThreadException("Execution of lookup query " + lookupQueryName + " with arguments "
        + params + " took too long to respond");
  }

  private boolean isAllParametersNull(Map<String, Object> params) {
    String[] lookupConds = lookupQueryParams;
    for (int i = 0; i < lookupConds.length; i++) {
      // These should be all "resolved" values so null is null for all
      // special null definitions, etc but our field could still
      // be "blank" instead of null
      Object value = params.get(lookupConds[i]);
      if (value instanceof String && value.toString().equals("")) {
        continue;
      } else if (value != null) {
        return false;
      }
    }
    return true;
  }

  private Object getLookupThroughInsert(final SqlMapClient client, Map<String, Object> params, ILoadingProcess proc)
      throws SQLException, InvalidExecutionThreadException {
    // These 2 lines of code below is just for backward compatibility, where by
    // lookup
    // inserts are always assumed to be on ref tables with field called "CODE"
    // leaving the lines here ensures older insert lookup queries work
    Object value = params.get(field);
    params.put("CODE", value);
    // end of backward compatibility code

    Object lookedUp = null;
    try {
      // Inform the executing thread that there is a db call being made
      proc.startDbCall(lookupInsertName);
      lookedUp = client.insert(lookupInsertName, params);
      // Inform the executing thread that the db call has been completed
      proc.endDbCall();
      if (!proc.isValid()) {
        throw new InvalidExecutionThreadException("Execution of lookup insert " + lookupInsertName + " with arguments "
            + params + " took too long to respond");
      }
    } catch (SQLException e) {
      // Inform the executing thread that the db call has been completed, even
      // if it has errors
      proc.endDbCall();
      if (!proc.isValid()) {
        throw new InvalidExecutionThreadException("Execution of lookup insert " + lookupInsertName + " with arguments "
            + params + " took too long to respond", e);
      }
      throw e;
    }
    getLog().emit(DataLoaderLookupLogClient.INSERTED, field, value);
    return lookedUp;
  }

  public int getMaximumCacheSize() {
    return maximumCacheSize;
  }

  public void setMaximumCacheSize(int maximumCacheSize) {
    this.maximumCacheSize = maximumCacheSize;
  }
}
