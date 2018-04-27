package com.profitera.dc;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import com.profitera.dc.msxls.impl.ExcelSpreadsheetRowDispenser;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.util.IRecordDispenser;

public final class RecordDispenserFactory {

  public static final String DELIMITED = "del";
  public static final String FIXED_WIDTH = "fixed";
  public static final String XML = "xml";
  public static final String DELIMITED_KEY_VALUE = "delkeyvalue";
  public static final String MSXLS = "msxls";
  public static final String LINE_TEXT = "LINE_TEXT";

  private RecordDispenserFactory() {
  }

  public static IRecordDispenser<?> getRecordDispenser(File file, int recordsToDispense, String beginMarker,
      String xquery, LoadDefinition definition, Charset charset) throws FileNotFoundException, IOException {
    String fileType = definition.getLoadType();
    if (fileType.equals(RecordDispenserFactory.FIXED_WIDTH)
        || fileType.equals(RecordDispenserFactory.DELIMITED)
        || fileType.equals(RecordDispenserFactory.DELIMITED_KEY_VALUE)) {
      return new LineDispenser(new BufferedReader(new InputStreamReader(new FileInputStream(file), charset)),
          recordsToDispense, file.getName());
    }
    if (fileType.equals(RecordDispenserFactory.XML)) {
      if (xquery == null || xquery.trim().length() == 0) {
        return new XMLRecordDispenser(new BufferedInputStream(new FileInputStream(file)), recordsToDispense,
            file.getName(), beginMarker);
      } else {
        return new XMLRecordDispenser(new BufferedInputStream(new FileInputStream(file)), recordsToDispense,
            file.getName(), beginMarker, xquery);
      }
    }
    if (fileType.equals(MSXLS)) {
      return new ExcelSpreadsheetRowDispenser(file, definition.getSelectedSheet(), recordsToDispense);
    }
    return null;
  }
}
