package com.profitera.dc.handler;

public interface IFieldTextHandlerContext {
  String getCurrentFieldName();
  long getCurrentLineNumber();
  String getLoadName();
}
