package com.profitera.dc.lookup.impl;

import com.profitera.log.ILogClient;
import com.profitera.log.ILogProvider;
import com.profitera.log.ILogProvider.Level;

public class DataLoaderLookupLogClient implements ILogClient {
  public static final String PARAM_ERROR = "LOOKUP_PARAM_ERROR";
  public static final String EXEC_FULL_CACHE = "LOOKUP_EXEC_FULL_CACHE";
  public static final String EXEC_FULL_CACHE_DONE = "LOOKUP_EXEC_FULL_CACHE_COMPLETE";
  public static final String EXEC_FULL_CACHE_ERROR = "LOOKUP_EXEC_FULL_CACHE_ERROR";
  public static final String CACHE_RESOLVED_NULL = "LOOKUP_RESOLVED_NULL_CACHE";
  public static final String CACHE_RESOLVED = "LOOKUP_RESOLVED_CACHE";
  public static final String NO_INSERT = "LOOKUP_NOT_INSERTED";
  public static final String NOT_RESOLVED = "LOOKUP_NOT_RESOLVED";
  public static final String RESOLVED = "LOOKUP_RESOLVED";
  public static final String INSERTED = "LOOKUP_INSERTED";

  @Override
  public void registerMessages(ILogProvider provider) {
    provider.registerMessage(this, PARAM_ERROR, Level.E, "Failed to retrieve lookup parameters for field {0}",
        "An error occured parsing lookup parameters.");
    provider.registerMessage(this, EXEC_FULL_CACHE, Level.I, "Executing query for full cache for field {0}",
        "Executing query for full cache for the field indicated.");
    provider.registerMessage(this, EXEC_FULL_CACHE_DONE, Level.I, "Completed query for full cache for field {0}",
        "Completed query for full cache for the field indicated.");
    provider.registerMessage(this, EXEC_FULL_CACHE_ERROR, Level.E,
        "Failed to execute query for full cache for field {0}",
        "An error occurred executing query for full cache for the field indicated.");
    provider.registerMessage(this, CACHE_RESOLVED_NULL, Level.D,
        "Lookup value for {0} [{1}] resolved to null from cache", "Field value resolved to null in cache.");
    provider.registerMessage(this, CACHE_RESOLVED, Level.D, "Lookup value for {0} [{1}] resolved to {2} from cache",
        "Field value resolved to the value indicated in cache.");
    provider.registerMessage(this, NO_INSERT, Level.W, "Lookup value for {0} [{1}] not inserted, all parameters null",
        "Insert aborted due to all parameters being null.");
    provider.registerMessage(this, NOT_RESOLVED, Level.W, "Lookup value for optional {0} [{1}] not found",
        "Lookup value not found.");
    provider.registerMessage(this, RESOLVED, Level.D, "Lookup value for {0} [{1}] resolved to {2}",
        "Field value resolved to the value indicated from the database.");
    provider.registerMessage(this, INSERTED, Level.I, "Created value for lookup {0} with text value {1}",
        "Field value created for missing lookup value.");
  }

  @Override
  public String getName() {
    return "Data Loader Lookup";
  }

}
