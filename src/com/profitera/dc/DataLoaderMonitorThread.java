package com.profitera.dc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataLoaderMonitorThread extends Thread {
  private final List<DataLoaderExecutionThread> activeThreads = new ArrayList<DataLoaderExecutionThread>();
  private final List<DataLoaderExecutionThread> invalidThreads = new ArrayList<DataLoaderExecutionThread>();
  private final List<DataLoaderExecutionThread> deadThreads = new ArrayList<DataLoaderExecutionThread>();

  private long timeoutDuration = 0;

  private static final Log LOG = LogFactory.getLog(DataLoaderMonitorThread.class);

  public DataLoaderMonitorThread(List<DataLoaderExecutionThread> threads, long timeoutDuration) {
    activeThreads.addAll(threads);
    if (timeoutDuration > 0) {
      this.timeoutDuration = timeoutDuration * 1000;
    }
  }

  public DataLoaderMonitorThread(DataLoaderExecutionThread[] threads, long timeoutDuration) {
    this(Arrays.asList(threads), timeoutDuration);
  }

  public void run() {
    for (int i = 0; i < activeThreads.size(); i++) {
      DataLoaderExecutionThread det = activeThreads.get(i);
      det.start();
    }
    while (executing()) {
      try {
        Thread.sleep(timeoutDuration/3L);
      } catch (InterruptedException e) {
        // Ignore this and carry on
      }
      validate();
      if (timeoutDuration > 0) {
        LOG.debug(getLogHeader() + "There are " + getNumberOfDeadThreads() + " completed, "
            + getNumberOfAliveButInvalidThreads() + " alive but invalid and " + getNumberOfActiveThreads()
            + " active thread(s)");
      }
    }
  }

  public void validate() {
    if (timeoutDuration <= 0) {
      return;
    }
    for (int i = 0; i < activeThreads.size(); i++) {
      DataLoaderExecutionThread det = activeThreads.get(i);
      if (det.getExecutionTime() > timeoutDuration) {
        det.setToInvalid();
        activeThreads.remove(det);
        if (det.isAlive()) {
          invalidThreads.add(det);
        }
        LOG.warn(getLogHeader() + "Loading thread " + det.getName() + " has been tagged invalid, "
            + det.getNumberOfInvalidLines() + " line(s) of record rejected");
      } else if (!det.isAlive()) {
        activeThreads.remove(det);
        deadThreads.add(det);
      }
    }
    for (int i = 0; i < invalidThreads.size(); i++) {
      DataLoaderExecutionThread det = invalidThreads.get(i);
      if (!det.isAlive()) {
        invalidThreads.remove(det);
        deadThreads.add(det);
      }
    }
  }

  public boolean executing() {
    for (int i = 0; i < activeThreads.size(); i++) {
      DataLoaderExecutionThread det = activeThreads.get(i);
      if (det.isAlive() && det.isValid()) {
        return true;
      }
    }
    return false;
  }

  public List<DataLoaderExecutionThread> getInvalidLiveThreads() {
    return invalidThreads;
  }

  public int getNumberOfDeadThreads() {
    return deadThreads.size();
  }

  public int getNumberOfAliveButInvalidThreads() {
    return invalidThreads.size();
  }

  public int getNumberOfActiveThreads() {
    return activeThreads.size();
  }

  public String getLogHeader() {
    return "[DataLoaderMonitoringThread] ";
  }
}
