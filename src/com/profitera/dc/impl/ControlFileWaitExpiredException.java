package com.profitera.dc.impl;

public class ControlFileWaitExpiredException extends Exception {
  private static final long serialVersionUID = 1L;
  public ControlFileWaitExpiredException(String string) {
    super(string);
  }
}
