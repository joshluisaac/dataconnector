package com.profitera.dc.impl;

import java.util.Map;

import com.profitera.dc.ILoadRowResolver;
import com.profitera.dc.ILoadingProcess;
import com.profitera.dc.InvalidExecutionThreadException;

public class StaticLoadRowResolver implements ILoadRowResolver {

  private final boolean isPresent;
  public StaticLoadRowResolver(boolean isPresent) {
    this.isPresent = isPresent;
  }
  public boolean isRecordPresent(Map m, long lineNo, ILoadingProcess thread)
      throws InvalidExecutionThreadException {
    return isPresent;
  }

}
