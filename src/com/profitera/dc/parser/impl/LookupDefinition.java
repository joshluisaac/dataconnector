package com.profitera.dc.parser.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.profitera.dc.InvalidLookupQueryException;
import com.profitera.util.Strings;

public class LookupDefinition {

  private String lookupQuery;
  private String[] lookupQueryParams;
  private String lookupFullCacheQuery;
  private String lookupInsertQuery;
  private String lookupInsertGenKey;
  private boolean lookupCache = true;
  private boolean lookupOptional = false;
  private Integer maximumCacheSize = null;
  
  public LookupDefinition(String lookupQuery){
    this.lookupQuery = lookupQuery;
  }
  
  private List<String> extractFieldsFromLookupQuery(String qry) throws InvalidLookupQueryException{
    String originalQuery = qry;
    if (qry == null) {
      return new ArrayList<String>();
    }
    List<String> fields = new ArrayList<String>();
    while (qry.indexOf('#') > -1){
      int loc = qry.indexOf('#');
      int loc2 = qry.indexOf('#', loc + 1);
      if (loc < 0 || loc2 < 0 || (loc2 - loc) < 2)
        throw new InvalidLookupQueryException(originalQuery);
      String field = qry.substring(loc + 1, loc2);
      if(Strings.nullifyIfBlank(field)==null){
        throw new InvalidLookupQueryException(originalQuery);
      }
      fields.add(field);
      qry = qry.substring(loc2 + 1, qry.length());
    }
    return cleanFields(fields, originalQuery);
  }
  
  private List cleanFields(List dirtyFields, String query) throws InvalidLookupQueryException{
    if (dirtyFields == null || dirtyFields.size() == 0)
      return dirtyFields;
    List cleanFields = new ArrayList();
    for (int i = 0; i < dirtyFields.size(); i++){
      String tmp = (String)dirtyFields.get(i);
      if (tmp != null && tmp.indexOf(':') > 0)
        tmp = tmp.substring(0, tmp.indexOf(':'));
      if(Strings.nullifyIfBlank(tmp)==null){
        throw new InvalidLookupQueryException(query);
      }
      cleanFields.add(tmp);
    }
    return cleanFields;
  }
  
  public String getLookupQuery() {
    return lookupQuery;
  }

  public String[] getLookupQueryParams() throws InvalidLookupQueryException {
    if(lookupQueryParams==null){
      List<String> params = extractFieldsFromLookupQuery(lookupQuery);
      if (hasLookupInsert()) {
        List<String> insertParams = extractFieldsFromLookupQuery(getLookupInsertQuery());
        for (String p : insertParams) {
          // We exclude ID because it is the name given to the generated ID internally,
          // every other parameter should come from the loading row data itself.
          if (!"ID".equals(p) && !params.contains(p)) {
            params.add(p);
          }
        }
      }
      this.lookupQueryParams = params.toArray(new String[0]);
      
    }
    return lookupQueryParams;
  }

  public String getLookupFullCacheQuery() {
    return lookupFullCacheQuery;
  }

  public String getLookupInsertQuery() {
    return lookupInsertQuery;
  }

  public String getLookupInsertGenKey() {
    return lookupInsertGenKey;
  }

  public boolean isLookupCache() {
    return lookupCache;
  }

  public boolean isLookupOptional() {
    return lookupOptional;
  }

  public void setLookupFullCacheQuery(String lookupFullCacheQuery) {
    this.lookupFullCacheQuery = Strings.nullifyIfBlank(lookupFullCacheQuery);
  }

  public void setLookupInsertQuery(String lookupInsertQuery, String lookupInsertGenKey) {
    if(Strings.nullifyIfBlank(lookupInsertQuery)!=null){
      this.lookupInsertQuery = lookupInsertQuery;
      this.lookupInsertGenKey = Strings.nullifyIfBlank(lookupInsertGenKey);
    }
  }

  public void setLookupCache(boolean lookupCache) {
    this.lookupCache = lookupCache;
  }

  public void setLookupOptional(boolean lookupOptional) {
    this.lookupOptional = lookupOptional;
  }

  public Integer getMaximumCacheSize() {
    return maximumCacheSize;
  }
  public void setMaximumCacheSize(Integer v) {
    maximumCacheSize = v;
  }

  public boolean hasLookupInsert() {
    return getLookupInsertQuery() != null;
  }
}
