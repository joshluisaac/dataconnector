package com.profitera.dc;

import java.util.List;

public interface ILoadingProcess {

  public abstract void acquireSemaphorePermit();

  public abstract void releaseSemaphorePermit(boolean isInvalid);

  public abstract void releaseSemaphorePermit();

  public abstract boolean isHoldingSemaphorePermit();

  public abstract void startDbCall(String queryName);

  public abstract void endDbCall();

  public abstract long getExecutionTime();

  public abstract boolean isValid();

  public abstract void setToInvalid();

  public abstract void setProcessingLines(List lines);

  public abstract int getNumberOfInvalidLines();

  public abstract String getLogHeader();

}