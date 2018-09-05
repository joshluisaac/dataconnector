package com.profitera.dc.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public abstract class FileHandleManager {
  private List<File> tempFilesToDelete = new ArrayList<File>();
  private List<BufferedWriter> writers = new ArrayList<BufferedWriter>();
  private List<PrintStream> printers = new ArrayList<PrintStream>();
  private List<FileOutputStream> outputs = new ArrayList<FileOutputStream>();
  public void withManager() throws IOException {
    try {
      with(this);
    } finally {
      cleanUp();
    }
  }
  private void cleanUp() {
    for (BufferedWriter w : writers) {
      try {
        w.flush();
        w.close();
      } catch (Throwable t) {
        // Nothing to do here, we just want to continue
      }
    }
    for (PrintStream w : printers) {
      try {
        w.flush();
        w.close();
      } catch (Throwable t) {
        // Nothing to do here, we just want to continue
      }
    }
    for (FileOutputStream w : outputs) {
      try {
        w.flush();
        w.close();
      } catch (Throwable t) {
        // Nothing to do here, we just want to continue
      }
    }
    // We closed first because some of the open streams
    // might be on the temp files created.
    for (File f : tempFilesToDelete) {
      f.delete();
    }
  }
  protected abstract void with(FileHandleManager m) throws IOException;
  public synchronized File getTemporaryFile(String pref, String suff) throws IOException {
    File f = File.createTempFile(pref, suff);
    tempFilesToDelete.add(f);
    return f;
  }
  public void registerTempFile(File f) {
    tempFilesToDelete.add(f);
  }

  public synchronized BufferedWriter getWriter(File f) throws IOException {
    BufferedWriter w = new BufferedWriter(new FileWriter(f, true));
    writers.add(w);
    return w;
  }
  public synchronized BufferedWriter getWriter(File f, boolean append) throws IOException {
    BufferedWriter w = new BufferedWriter(new FileWriter(f, append));
    writers.add(w);
    return w;
  }

  public PrintStream getPrintWriter(File stacktrace, Charset c) throws FileNotFoundException, UnsupportedEncodingException {
    PrintStream printStream = new PrintStream(new FileOutputStream(stacktrace, true), true, c.name());
    // Printstream closes the underlying stream when it is closed, so we do not need
    // to manage that too.
    printers.add(printStream);
    return printStream;
  }
  public OutputStream getOutputStream(File f) throws FileNotFoundException {
    FileOutputStream fo = new FileOutputStream(f);
    outputs.add(fo);
    return fo;
  }
}
