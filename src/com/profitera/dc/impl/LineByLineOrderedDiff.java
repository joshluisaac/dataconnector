package com.profitera.dc.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Comparator;

import com.profitera.io.StreamUtil;
import com.profitera.util.io.FileUtil;

public class LineByLineOrderedDiff {
  private final Charset charset;
  private final String lineSeparator;

  public LineByLineOrderedDiff(Charset charset) {
    this.charset = charset;
    lineSeparator = System.getProperty("line.separator");
  }

  public void verifyKeySortOrder(File f, Comparator<String> comparator) throws IOException, UnorderedFileKeyException {
    InputStream stream = new FileInputStream(f);
    BufferedReader lineReader = new BufferedReader(new InputStreamReader(stream, charset));
    int lineNo = 0;
    try {
      lineNo++;
      String line1 = lineReader.readLine();
      if (line1 == null) {
        // No lines means ordered
        return;
      }
      lineNo++;
      String line2 = lineReader.readLine();
      while (line2 != null) {
        if (comparator.compare(line1, line2) > 0) {
          throw new UnorderedFileKeyException("Input file " + FileUtil.tryCanonical(f) + " keys found to not be in sorted order at line " + lineNo);
        }
        lineNo++;
        line1 = line2;
        line2 = lineReader.readLine();
      }
    } finally {
      StreamUtil.closeFinally(stream);
      StreamUtil.closeFinally(lineReader);
    }
  }
  public void performDiff(File newFile, File previousFile, Comparator<String> keyComparator, File deltaFile)
      throws FileNotFoundException, IOException {
    OutputStream out = null;
    OutputStreamWriter w = null;
    InputStream newFileInputStream = new FileInputStream(newFile);
    InputStream oldFileInputStream = new FileInputStream(previousFile);
    try {
      out = new FileOutputStream(deltaFile);
      w = new OutputStreamWriter(out, charset);
      performDiff(newFileInputStream, oldFileInputStream, keyComparator, w);
      
    } finally {
      StreamUtil.closeFinally(newFileInputStream);
      StreamUtil.closeFinally(oldFileInputStream);
      w.flush();
      StreamUtil.closeFinally(w);
      StreamUtil.closeFinally(out);
    }
  }
  private void performDiff(InputStream newFileInputStream, InputStream oldFileInputStream, Comparator<String> keyComparator, OutputStreamWriter deltaOut)
      throws FileNotFoundException, IOException {
    BufferedReader newFileBuffered = new BufferedReader(new InputStreamReader(newFileInputStream, charset));
    BufferedReader oldFileBuffered = new BufferedReader(new InputStreamReader(oldFileInputStream, charset));
    try {
      String currentNew = newFileBuffered.readLine();
      String currentOld = oldFileBuffered.readLine();
      while (true) {
        if (currentOld == null) {
          while (currentNew != null) {
            addToDiffOutput(currentNew, deltaOut);
            currentNew = newFileBuffered.readLine();
          }
          return;
        } else if (currentNew == null) {
          return;
        }
        int keysCompareResult = keyComparator.compare(currentNew, currentOld);
        if (currentNew.equals(currentOld)) {
          // Exact match found, advance both streams:
          currentNew = newFileBuffered.readLine();
          currentOld = oldFileBuffered.readLine();
        } else if (keysCompareResult > 0) {
          currentOld = oldFileBuffered.readLine();
        } else {
          addToDiffOutput(currentNew, deltaOut);
          currentNew = newFileBuffered.readLine();
        }
      }
    } finally {
      StreamUtil.closeFinally(newFileBuffered);
      StreamUtil.closeFinally(oldFileBuffered);
    }
  }

  private void addToDiffOutput(String currentNew, OutputStreamWriter deltaOut) throws IOException {
    deltaOut.write(currentNew);
    deltaOut.write(lineSeparator);
  }
}
