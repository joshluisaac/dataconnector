package com.profitera.dc.impl;

import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.profitera.dc.ILoadRowResolver;
import com.profitera.dc.ILoadingProcess;
import com.profitera.dc.InvalidExecutionThreadException;
import com.profitera.dc.QueryExistingRowException;
import com.profitera.dc.lookup.ParameterCache;

public class QueryLoadRowResolver implements ILoadRowResolver {

  private final ParameterCache rowLookupCache;
  private final Log log = LogFactory.getLog(getClass());
  private final String selectName;
  private final SqlMapClient reader;

  public QueryLoadRowResolver(String selectName, ParameterCache cache, SqlMapClient reader) {
    this.selectName = selectName;
    this.rowLookupCache = cache;
    this.reader = reader;
  }
  
  public boolean isRecordPresent(Map m, long lineNo, ILoadingProcess thread) throws InvalidExecutionThreadException {
    Number o = null;
    if (rowLookupCache != null){
      Object temp = rowLookupCache.getFromCache(m);
      o = (Number) temp;
      if (o != null){
        log.debug("Resolved from row cache for " + m);
      }
    } else {
      log.debug(m);
      try {
        //Tell the thread that we're making db query again
        thread.startDbCall(selectName);
        o = (Number) reader.queryForObject(selectName, m);
        //Inform the executing thread that the db call has been completed
        thread.endDbCall();
        if (!thread.isValid())
          throw new InvalidExecutionThreadException("Execution of select query " + selectName + " with arguments " + m + " took too long to respond");
        
      } catch (SQLException e){
        //Inform the executing thread that the db call has been completed, even if it caused an error
        thread.endDbCall();
        if (!thread.isValid()) {
          throw new InvalidExecutionThreadException("Execution of select query " + selectName + " with arguments " + m + " took too long to respond");
        }
        throw new QueryExistingRowException("Failed to execute " + selectName, e, lineNo);
      }
    }
    if (o == null || o.longValue() == 0){
      return false;
    } else {
      return true;
    }
  }
}
