package com.profitera.dc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.profitera.dataaccess.SqlMapProvider;

public class DbRecordDispenser {
  private SqlMapProvider reader = null;
  private int perRequest = 1;
  private int skip = 0;
  private Object mutex = new Object();
  private int currentPos = 0;

  public static final String ALL_RECORDS = "records";
  public static final String WRITES_FAILED = "writeFails";
  public static final String UNKNOWN_FAILED = "unknownFails";
  public static final String QUERY_FAILED = "queryFailed";
  public static final String CONDITIONS_FAILED = "queryFailed";
  public static final String CAPTURED_EXCEPTIONS = "capturedExceptions";
 
  private Iterator records = null;
  
  private DbRecordDispenser(){}
  
  public DbRecordDispenser(SqlMapProvider sqlClient, int recordsPerFetch, int recordsToSkip){
    reader = sqlClient;
    perRequest = recordsPerFetch;
    skip = recordsToSkip;
  }
  
  public void executeFetch(String query, Map args) throws SQLException{
    records = reader.query(SqlMapProvider.STREAM, query, args);
  }
  
  public int dispenseRecords(final List fillMe) throws IOException{
    fillMe.clear();
    if (records == null)
      return 0;
    synchronized (mutex) {
      int currentFetch = 0;
      if (records == null)
        return 0;
      while (records.hasNext()){
      	fillMe.add((Map)records.next());
      	currentFetch++;
      	currentPos++;
      	if (currentFetch == perRequest){
      		if (skip > 0){
	      		int skipped = 0;
	      		while (records.hasNext() && skipped < skip){
	      			records.next();
	      			skipped++;
	      		}
      		}
      		break;
      	}
      }
    }
    return currentPos;
  }
  
  public int getCurrentRecordPosition(){
  	return currentPos;
  }
}