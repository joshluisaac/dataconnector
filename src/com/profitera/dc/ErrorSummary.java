package com.profitera.dc;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.profitera.dc.impl.FileHandleManager;
import com.profitera.util.PrimitiveValue;
import com.profitera.util.Utilities;

public class ErrorSummary {
  private enum LineCount {
    ALL, UNKOWN_FAIL, PARSE_FAIL, INTEGRITY_FAIL, TOO_LONG_FAIL, FILTERED, COMMIT_FAIL, HEADER_TRAILER
  };

  private Map<LineCount, Long> counts = new HashMap<LineCount, Long>();
  private String filteredLinesFilePath;
  private String failedLinesFilePath;
  private String exceptionsFilePath;
  private final FileHandleManager fileManager;

  public ErrorSummary(FileHandleManager fm) {
    this.fileManager = fm;
  }

  public List<String> getFilteredLines() throws IOException {
    return readFromDisk(filteredLinesFilePath);
  }

  private String storeLines(List lines, String lastPath, String ext) throws IOException {
    removeIfExist(lastPath);
    if (lines == null || lines.isEmpty()) {
      return null;
    }
    File f = fileManager.getTemporaryFile(new Random().nextLong() + "", ext);
    printToDisk(lines, f);
    return f.getAbsolutePath();
  }

  public void setFilteredLines(List<String> filteredLines) throws IOException {
    filteredLinesFilePath = storeLines(filteredLines, filteredLinesFilePath, ".filter");
  }

  public List<Exception> getExceptions() throws IOException {
    if (exceptionsFilePath != null) {
      return readFromDisk(exceptionsFilePath);
    }
    return null;
  }

  public void setExceptions(List exceptions) throws IOException {
    exceptionsFilePath = storeLines(exceptions, exceptionsFilePath, ".exception");
  }

  public List<String> getFailedLines() throws IOException {
    return readFromDisk(failedLinesFilePath);
  }

  public void setFailedLines(List<String> failedLines) throws IOException {
    failedLinesFilePath = storeLines(failedLines, failedLinesFilePath, ".bad");
  }

  private void removeIfExist(String path) {
    if (path != null) {
      File f = new File(path);
      if (f.exists()) {
        f.delete();
      }
    }
  }

  private List readFromDisk(String path) throws IOException {
    try {
      return (List) Utilities.read(path);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private void printToDisk(List lines, File f) throws IOException {
    Utilities.write(lines, f);
  }

  public long getAllLines() {
    return getCount(LineCount.ALL);
  }

  public void setAllLines(long allLines) {
    counts.put(LineCount.ALL, allLines);
  }

  public long getUnknownError() {
    return getCount(LineCount.UNKOWN_FAIL);
  }

  public void setUnknownError(long unknownError) {
    counts.put(LineCount.UNKOWN_FAIL, unknownError);
  }

  public long getParsingError() {
    return getCount(LineCount.PARSE_FAIL);
  }

  private long getCount(LineCount t) {
    return PrimitiveValue.longValue(counts.get(t), 0);
  }

  public void setParsingError(long parsingError) {
    counts.put(LineCount.PARSE_FAIL, parsingError);
  }

  public long getDataIntegrityError() {
    return getCount(LineCount.INTEGRITY_FAIL);
  }

  public void setDataIntegrityError(long dataIntegrityError) {
    counts.put(LineCount.INTEGRITY_FAIL, dataIntegrityError);
  }

  public long getDbRequestTooLong() {
    return getCount(LineCount.TOO_LONG_FAIL);
  }

  public void setDbRequestTooLong(long dbRequestTooLong) {
    counts.put(LineCount.TOO_LONG_FAIL, dbRequestTooLong);
  }

  public long getFiltered() {
    return getCount(LineCount.FILTERED);
  }

  public void setFiltered(long filtered) {
    counts.put(LineCount.FILTERED, filtered);
  }
  public long getHeaderTrailer() {
    return getCount(LineCount.HEADER_TRAILER);
  }

  public void addHeaderTrailerLine() {
    counts.put(LineCount.HEADER_TRAILER, getHeaderTrailer() + 1L);
  }
  public void setHeaderTrailer(long count) {
    counts.put(LineCount.HEADER_TRAILER, count);
  }


  public long getCommitFailed() {
    return getCount(LineCount.COMMIT_FAIL);
  }

  public void setCommitFailed(long commitFailed) {
    counts.put(LineCount.COMMIT_FAIL, commitFailed);
  }
}
