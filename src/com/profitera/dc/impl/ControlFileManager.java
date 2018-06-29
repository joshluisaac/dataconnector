package com.profitera.dc.impl;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.profitera.log.DefaultLogProvider;
import com.profitera.util.Strings;
import com.profitera.util.io.FileUtil;

public class ControlFileManager {
  private static final int ONE_HUNDRED_MS = 100;
  private static final int ONE_MINUTE = 60000;
  private final File sourceFile;
  private final String controlFileName;
  private final String loadName;
  public ControlFileManager(String loadName, File file, String waitFileExt, String waitFileReplace) {
    this(loadName, file, waitFileExt, waitFileReplace, null, null);
  }
  public ControlFileManager(String loadName, File file, String controlFilePattern, Date d) {
    this(loadName, file, null, null, controlFilePattern, d);
  }
  private ControlFileManager(String loadName, File file, String waitFileExt, String waitFileReplace, String overrideWithThisPattern, Date currentTime) {
    sourceFile = file;
    this.loadName = loadName;
    String fileName = file.getName();
    if (Strings.nullifyIfBlank(overrideWithThisPattern) != null) {
      if (currentTime == null) {
        throw new IllegalArgumentException("Date required for use of control file path pattern");
      }
      SimpleDateFormat f = new SimpleDateFormat(overrideWithThisPattern);
      controlFileName = f.format(currentTime);
    } else if (Strings.nullifyIfBlank(waitFileReplace) == null) {
      controlFileName = fileName + Strings.coalesce(waitFileExt, "");
    } else {
      controlFileName = fileName.replaceFirst(waitFileReplace, waitFileExt);
    }
  }

  public void waitForFile(long duration) throws ControlFileWaitExpiredException {
    if (duration == 0) {
      return;
    }
    long waitInterval = ONE_MINUTE;
    if (duration <= ONE_MINUTE) {
      waitInterval = ONE_HUNDRED_MS;
    }
    File controlFile = getControlFile();
    long start = System.currentTimeMillis();
    while (duration > (System.currentTimeMillis() - start)) {
      DefaultLogProvider l = new DefaultLogProvider();
      l.register(new DataLoaderLogClient());
      l.emit(DataLoaderLogClient.WAIT_CONTROL_FILE, FileUtil.tryCanonical(controlFile), getLoadName(), new Date(start + duration));
      if (controlFile.exists()) {
        break;
      }
      try {
        Thread.sleep(waitInterval);
      } catch (InterruptedException e) {
      }
    }
    if (!controlFile.exists()) {
      throw new ControlFileWaitExpiredException("Wait expired, control file " + FileUtil.tryCanonical(controlFile) + " still not found.");
    }
  }

  private String getLoadName() {
    return loadName;
  }
  public File getControlFile() {
    return new File(sourceFile.getParentFile(), controlFileName);
  }
}
