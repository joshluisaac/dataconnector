package com.profitera.dc.impl;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import com.profitera.dc.RecordDispenserFactory;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.log.ILogProvider;
import com.profitera.sort.ExternalSort;
import com.profitera.util.io.FileUtil;

public class DeltaGenerator {
  private final LoadDefinition definition;
  private ILogProvider log;
  private final String loadName;
  private final Charset charset;

  public DeltaGenerator(String loadName, LoadDefinition definition, ILogProvider p, Charset charset) {
    this.loadName = loadName;
    this.definition = definition;
    this.log = p;
    this.charset = charset;
  }

  public File generateDeltaFile(File todaysFile, File previousDayFile, FileHandleManager m, boolean doSortOriginalFiles) {
    //TODO: Use handle manager here.
    String fileType = definition.getLoadType();
    if (!fileType.equals(RecordDispenserFactory.FIXED_WIDTH)) {
      return todaysFile;
    }
    if (previousDayFile == null || !previousDayFile.exists()) {
      return todaysFile;
    }
    LineByLineOrderedDiff d = new LineByLineOrderedDiff(charset);
    Comparator<String> comparator = new SubstringStringComparator(definition);
    try {
      d.verifyKeySortOrder(todaysFile, comparator);
      d.verifyKeySortOrder(previousDayFile, comparator);
      File deltaFile = new File(todaysFile.getParentFile(), todaysFile.getName() + ".delta");
      d.performDiff(todaysFile, previousDayFile, comparator, deltaFile);
      return deltaFile;
    } catch (UnorderedFileKeyException e) {
      if (doSortOriginalFiles) {
        getLog().emit(DataLoaderLogClient.ATTEMPT_SORT, e.getMessage(), getLoadName());
        try {
          sortFileInPlace(todaysFile, charset, comparator);
          sortFileInPlace(previousDayFile, charset, comparator);
          return generateDeltaFile(todaysFile, previousDayFile, m, false);
        } catch (IOException e1) {
          getLog().emit(DataLoaderLogClient.NO_DELTA, e1, getLoadName());
        }
      } else {
        getLog().emit(DataLoaderLogClient.NO_DELTA, e, getLoadName());
      }
    } catch (IOException e) {
      getLog().emit(DataLoaderLogClient.NO_DELTA, e, getLoadName());
    }
    return todaysFile;
  }

  public void sortFileInPlace(File originalFile, Charset charset, Comparator<String> comparator) throws IOException {
    getLog().emit(DataLoaderLogClient.ATTEMPT_SORT_FILE, FileUtil.tryCanonical(originalFile), new Date(originalFile.lastModified()), getLoadName());
    List<File> forMerging = ExternalSort.sortInBatch(originalFile, comparator, 512, charset, null, true);
    ExternalSort.mergeSortedFiles(forMerging, originalFile, comparator, charset, true, false, false);
  }

  private String getLoadName() {
    return loadName;
  }

  private ILogProvider getLog() {
    return log;
  }

}
