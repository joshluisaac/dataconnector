package com.profitera.dc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataLoaderExecutionThread extends Thread implements ILoadingProcess {

	private List lines = null;
	private int noOfInvalidLines = 0;
	private boolean valid = true;
	private long executionTime = 0;
	
	private Semaphore semaphore = null;
	private boolean isHoldingPermit = false;
	
  private static final Log log = LogFactory.getLog(DataLoaderExecutionThread.class);

  private String queryName = null;
  private final LoadingErrorList errorList;
  
	public DataLoaderExecutionThread (LoadingErrorList errorList, Semaphore semaphore) {
		this.errorList = errorList;
		this.semaphore = semaphore;
	}

	public synchronized void acquireSemaphorePermit() {
			try {
				semaphore.acquire();
				isHoldingPermit = true;
				log.debug(getLogHeader() + "Acquired permit from semaphore");
			} catch (InterruptedException e) {
				log.error(getLogHeader() + "Semaphore acquisition interrupted.  Weird.", e);
			}
	}
	
	public synchronized void releaseSemaphorePermit(boolean isInvalid) {
		if (isHoldingPermit) {
			semaphore.release();
			isHoldingPermit = false;
			log.debug(getLogHeader() + "Released permit to semaphore " + (isInvalid ? "forcefully" : "gracefully"));
		}
	}

	public void releaseSemaphorePermit() {
		releaseSemaphorePermit(false);
	}

	public synchronized boolean isHoldingSemaphorePermit(){
		return isHoldingPermit;
	}

	public void startDbCall(String queryName) {
		executionTime = System.currentTimeMillis();
		this.queryName = queryName;
		log.debug(getLogHeader() + "Started db call for query \"" + queryName + "\"");
	}
	
	public void endDbCall() {
		executionTime = 0;
		log.debug(getLogHeader() + "Ended db call for query \"" + queryName + "\"");
	}
	
	public long getExecutionTime() {
		if (executionTime == 0)
			return 0;
		return System.currentTimeMillis() - executionTime;
	}

	public boolean isValid() {
		return valid;
	}
	
	public void setToInvalid() {
		releaseSemaphorePermit(true);
		valid = false;
		errorList.writeBadLines(lines, log);
		List exceptionsList = new ArrayList();
		exceptionsList.add(new InvalidExecutionThreadException(getLogHeader() + "Has been waiting for " + getExecutionTime()/1000 + " secs for query \"" + queryName + "\" to complete.  Thats too long, thread is flagged invalid."));
		errorList.writeException(exceptionsList, log);
		lines.clear();
	}
	
	public void setProcessingLines(List lines) {
		this .lines = lines;
		noOfInvalidLines = lines.size();
	}
	
	public int getNumberOfInvalidLines() {
		return noOfInvalidLines;
	}
	
  public String getLogHeader() {
  	return "[" + this.getName() + "] ";
  }
}