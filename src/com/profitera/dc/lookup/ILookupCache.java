package com.profitera.dc.lookup;

import java.util.Map;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.profitera.dc.ILoadingProcess;
import com.profitera.dc.InvalidExecutionThreadException;
import com.profitera.dc.parser.impl.LookupDefinition;

public interface ILookupCache {
  public void configure(String name, LookupDefinition l, SqlMapClient reader, SqlMapClient writer);

  public Object getLookupValue(Map<String, Object> params, long line, ILoadingProcess proc)
      throws InvalidExecutionThreadException;

}
