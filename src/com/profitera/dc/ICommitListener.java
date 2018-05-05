package com.profitera.dc;

public interface ICommitListener {
  public void started(String loadName);
  public void committed(String loadName, long startLineNo, long endLineNumber);
  public void ended(String loadName, LoadingErrorList errors);
}
