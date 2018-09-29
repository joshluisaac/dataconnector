package com.profitera.dc.parser.impl;

public class GlobalFilter {

  private final LookupDefinition lookup;
  private final String name;

  public GlobalFilter(String filterName, LookupDefinition lookup) {
    this.name = filterName;
    this.lookup = lookup;
  }

  public String getName() {
    return name;
  }

  public LookupDefinition getLookup() {
    return lookup;
  }


}
