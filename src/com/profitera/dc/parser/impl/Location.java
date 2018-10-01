package com.profitera.dc.parser.impl;


public class Location {
  private final String start;
  private final String end;

  public Location(String start, String end) {
    if (start == null || start.trim().equals("")) {
      start = "0";
    } 
    if (end == null || end.trim().equals("")) {
      end = "0";
    }
    this.start = start;
    this.end = end;
  }
  
  public String getStart() {
    return start;
  }
  
  public String getEnd() {
    return end;
  }
}
