package com.profitera.dc;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;

import com.profitera.dc.impl.DataLoaderLogClient;
import com.profitera.log.DefaultLogProvider;
import com.profitera.log.ILogProvider;

public class LoadingErrorList {
  private List<ErrorSummary> errorList = Collections.synchronizedList(new ArrayList<ErrorSummary>());
  private final BufferedWriter badFile;
  private final PrintStream stacktraceFile;
  private final BufferedWriter filterFile;
  private boolean nativeLoaderFailure = false;
  private Integer notLoadedByNativeLoaderCount = null;
  private ILogProvider l;

  public LoadingErrorList(BufferedWriter badFile, PrintStream stacktraceFile, BufferedWriter filterFile) {
    this.badFile = badFile;
    this.stacktraceFile = stacktraceFile;
    this.filterFile = filterFile;
  }

  public void addErrors(ErrorSummary s) {
    errorList.add(s);
  }

  public void logFailureSummaries(String loadName, Log log, long duration) {
    logFailureSummary(loadName, getErrorSummary(), log, duration);
  }

  public ErrorSummary getErrorSummary() {
    long pFails = 0;
    long fFails = 0;
    long dFails = 0;
    long uFails = 0;
    long cFails = 0;
    long dbFails = 0;
    long headTrail = 0;
    long lines = 0;
    for (Iterator<ErrorSummary> i = errorList.iterator(); i.hasNext();) {
      ErrorSummary m = i.next();
      dFails += m.getDataIntegrityError();
      pFails += m.getParsingError();
      fFails += m.getFiltered();
      uFails += m.getUnknownError();
      cFails += m.getCommitFailed();
      dbFails += m.getDbRequestTooLong();
      headTrail += m.getHeaderTrailer();
      lines += m.getAllLines();
    }
    ErrorSummary total = new ErrorSummary(null);
    total.setParsingError(pFails);
    total.setFiltered(fFails);
    total.setDataIntegrityError(dFails);
    total.setUnknownError(uFails);
    total.setCommitFailed(cFails);
    total.setDbRequestTooLong(dbFails);
    total.setHeaderTrailer(headTrail);
    total.setAllLines(lines);
    return total;
  }

  private void logFailureSummary(String loadName, ErrorSummary total, Log log, long durationMillis) {
    long lines = total.getAllLines();
    log.info(total.getHeaderTrailer() + " of " + lines + " rejected as headers/footers");
    log.info(total.getParsingError() + " of " + lines + " failed due to parsing errors");
    log.info(total.getFiltered() + " of " + lines + " rejected due to not meeting filter conditions");
    log.info(total.getDataIntegrityError() + " of " + lines + " failed due to data integrity errors");
    log.info(total.getUnknownError() + " of " + lines + " failed due to unknown errors");
    log.info(total.getDbRequestTooLong() + " of " + lines
        + " failed due to database taking too long to process request");
    log.info(total.getCommitFailed() + " of " + lines + " failed due to failed database commits");
    long allUnloaded = getAllNotLoaded(total, log);
    log.info(allUnloaded + " of " + lines + " total failures and rejects");
    long ineligible = total.getFiltered() + total.getHeaderTrailer();
    long eligible = lines - ineligible;
    long loadedCount = eligible - (allUnloaded - ineligible);
    long seconds = durationMillis/1000;
    if (seconds == 0) {
      seconds = 1;
    }
    getLog().emit(DataLoaderLogClient.ELIGIBLE_LOADED, loadName, loadedCount, eligible, (durationMillis/1000), loadedCount/seconds);
    
  }

  public long getAllNotLoaded(ErrorSummary total, Log log) {
    long lines = total.getAllLines();
    long allUnloaded = total.getParsingError() + total.getFiltered() + total.getDataIntegrityError() + total.getUnknownError()
        + total.getDbRequestTooLong() + total.getCommitFailed() + total.getHeaderTrailer();
    if (nativeLoaderFailure) {
      // All possibly successful lines then failed when the native loader barfed on data
      long nativeFailure = lines - allUnloaded;
      if (log != null) {
        log.info(nativeFailure + " of " + lines + " failed due to native database loading failing to execute");
      }
      allUnloaded = lines;
    } else if (notLoadedByNativeLoaderCount != null) {
      if (log != null) {
        log.info(notLoadedByNativeLoaderCount + " of " + lines + " not loaded by native database loader");
      }
      allUnloaded = allUnloaded + notLoadedByNativeLoaderCount;
    }
    return allUnloaded;
  }

  public void writeLogDetails(Log log) {
    synchronized (errorList) {
      for (Iterator<ErrorSummary> i = errorList.iterator(); i.hasNext();) {
        ErrorSummary errorSummary = i.next();
        try {
          writeBadLines(errorSummary.getFailedLines(), log);
        } catch (IOException e) {
          log.error("Failed to write bad file", e);
        }
        try {
          writeException(errorSummary.getExceptions(), log);
        } catch (IOException e) {
          log.error("Failed to write stacktrace file", e);
        }
        try {
          writeFilteredLines(errorSummary.getFilteredLines(), log);
        } catch (IOException e) {
          log.error("Failed to write filter file", e);
        }
      }
    }
  }

  private synchronized void writeFilteredLines(List<String> lines, Log log) {
    if (lines == null || lines.size() == 0 || filterFile == null)
      return;

    for (int i = 0; i < lines.size(); i++) {
      try {
        filterFile.write(lines.get(i));
        filterFile.newLine();
        filterFile.flush();
      } catch (Exception e) {
        log.error("Error while trying to write into filter file");
        log.error("Error received: " + e);
        log.error("Error stacktrace:", e);
      }
    }
  }

  public synchronized void writeException(List<Exception> errors, Log log) {

    if (errors == null || errors.size() == 0 || stacktraceFile == null)
      return;

    try {
      for (int i = 0; i < errors.size(); i++) {
        Exception theError = errors.get(i);
        theError.printStackTrace(stacktraceFile);
        stacktraceFile.flush();
      }
    } catch (Exception ex) {
      log.warn("Error while trying to write stacktrace log file");
      log.warn("Error received: " + ex);
      log.warn("Error stacktrace:", ex);
    }
  }

  public synchronized void writeBadLines(List<?> lines, Log log) {
    if (lines == null || lines.size() == 0 || badFile == null)
      return;
    for (int i = 0; i < lines.size(); i++) {
      try {
        Object line = lines.get(i);
        if (!(line instanceof String)) {
          line = String.valueOf(line);
        }
        badFile.write((String)line);
        badFile.newLine();
        badFile.flush();
      } catch (Exception e) {
        log.error("Error while trying to write into bad file");
        log.error("Error received: " + e);
        log.error("Error stacktrace:", e);
      }
    }
  }

  public void writeException(Exception exception, Log log) {
    List<Exception> l = new ArrayList<Exception>();
    l.add(exception);
    writeException(l, log);
  }

  public List<ErrorSummary> getErrorList() {
    synchronized (errorList) {
      return errorList;
    }
  }

  public void setNativeLoaderFailure(boolean b) {
    this.nativeLoaderFailure = b;
  }

  public void setNativeLoaderNotLoaded(int notLoadedCount) {
    this.notLoadedByNativeLoaderCount = notLoadedCount;
  }

  private ILogProvider getLog() {
    if (l == null) {
      l = new DefaultLogProvider();
      l.register(new DataLoaderLogClient());
    }
    return l;
  }

  public long getLineCount() {
    return getErrorSummary().getAllLines();
  }

  public long getNonLoadedLineCount(Log log) {
    return getAllNotLoaded(getErrorSummary(), log);
  }

}
