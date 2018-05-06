package com.profitera.dc;

public class InvalidExecutionThreadException extends Exception {
  private static final long serialVersionUID = 1L;
  public InvalidExecutionThreadException (String msg) {
    super(msg);
  }
  public InvalidExecutionThreadException (String msg, Throwable t) {
    super(msg, t);
  }
}
