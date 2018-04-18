package com.profitera.dc;

import java.util.Map;

public interface ILoadRowResolver {

  public abstract boolean isRecordPresent(Map m, 
      long lineNo, ILoadingProcess thread) throws InvalidExecutionThreadException;

}