package com.profitera.dc.impl;

public class FieldParsingFormatException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  public FieldParsingFormatException(String message, NumberFormatException cause) {
    super(message, cause);
  }
}
