package com.profitera.dc;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import com.profitera.util.RecordDispenser;

public class LineDispenser extends RecordDispenser<String> {
  private final BufferedReader reader;

  public LineDispenser(BufferedReader r, int linesPerRequest, String fileName) {
    reader = r;
    perRequest = linesPerRequest;
    if (fileName != null && fileName.trim().length() != 0) {
      this.sourceName = fileName;
    }
  }

  public String getType() {
    return RecordDispenserFactory.LINE_TEXT;
  }

  protected int dispenseLinesPrivately(final List<String> fillMe) throws IOException {
    fillMe.clear();
    if (reader == null) {
      return 0;
    }
    synchronized (mutex) {
      if (reader == null) {
        return 0;
      }
      int startLine = currentRecord;
      String line = reader.readLine();
      currentRecord++;
      for (int requestLines = 1; line != null; requestLines++) {
        fillMe.add(line);
        if (requestLines >= perRequest) {
          break;
        }
        line = reader.readLine();
        currentRecord++;
      }
      if (!fillMe.isEmpty() && log.isDebugEnabled()) {
        log.debug("Dispensed " + startLine + " - " + (startLine + fillMe.size() - 1) + " from file " + sourceName + " to "
            + Thread.currentThread().getName());
      }
      return startLine;
    }
  }

  @Override
  public void terminate() {
    if (reader == null) {
      return;
    }
    try {
      reader.close();
    } catch (Throwable t) { // NOPMD
      // Ignore any error that might occur
    }
  }
}
