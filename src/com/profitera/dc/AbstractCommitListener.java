package com.profitera.dc;

public abstract class AbstractCommitListener implements ICommitListener {
  @Override
  public void committed(String loadName, long start, long end) {
    // Do nothing by default, if replaced then real stuff should happen
  }

  @Override
  public void started(String loadName) {
  }

  @Override
  public void ended(String loadName, LoadingErrorList list) {
  }
}