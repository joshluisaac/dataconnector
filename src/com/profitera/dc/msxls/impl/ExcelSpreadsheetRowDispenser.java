package com.profitera.dc.msxls.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.profitera.dc.RecordDispenserFactory;
import com.profitera.dcpoi3_9.hssf.usermodel.HSSFWorkbook;
import com.profitera.dcpoi3_9.openxml4j.exceptions.InvalidFormatException;
import com.profitera.dcpoi3_9.openxml4j.opc.OPCPackage;
import com.profitera.dcpoi3_9.poifs.filesystem.OfficeXmlFileException;
import com.profitera.dcpoi3_9.poifs.filesystem.POIFSFileSystem;
import com.profitera.dcpoi3_9.ss.usermodel.Row;
import com.profitera.dcpoi3_9.ss.usermodel.Sheet;
import com.profitera.dcpoi3_9.ss.usermodel.Workbook;
import com.profitera.dcpoi3_9.xssf.usermodel.XSSFWorkbook;
import com.profitera.util.RecordDispenser;

public class ExcelSpreadsheetRowDispenser extends RecordDispenser<Map<String, String>> {
  private final OPCPackage ooxmlPackage;

  /**
   * This accepts a file instead of a stream because of the way the
   * XML API works, since the Office XML format is wrapped in a ZIP
   * file it is better to pass in the file so it can be opened as a
   * native ZIP file instead.
   * @param f
   * @param perRequest
   * @throws IOException
   */
  public ExcelSpreadsheetRowDispenser(File f, String sheet, int perRequest) throws IOException {
    super.perRequest = perRequest;
    Workbook workBook = null;
    OPCPackage pkg = null;
    try {
      // POI Closes the stream after reading
      POIFSFileSystem fileSystem = new POIFSFileSystem(new FileInputStream(f));
      workBook = new HSSFWorkbook(fileSystem);
    } catch (OfficeXmlFileException e) {
      try {
        pkg = OPCPackage.open(f);
      } catch (InvalidFormatException e1) {
        throw new IOException(e1);
      }
      workBook = new XSSFWorkbook(pkg);
    }
    ooxmlPackage = pkg;
    Sheet fileSheet = workBook.getSheetAt(0);
    if (sheet != null) {
      fileSheet = workBook.getSheet(sheet);
      if (fileSheet == null) {
        try {
          int sheetIndex = Integer.valueOf(sheet);
          fileSheet = workBook.getSheetAt(sheetIndex + 1);
        } catch (NumberFormatException nfe) {
          throw new NumberFormatException("Unable to locate sheet with name '" + sheet + "' or use that name as a sheet index");
        }
      }
    }
    final Iterator<Row> rows = fileSheet.rowIterator();
    super.records = new ExcelSheetRowIterator(rows);
  }

  @Override
  public String getType() {
    return RecordDispenserFactory.MSXLS;
  }

  @Override
  public void terminate() {
    if (ooxmlPackage != null) {
      try {
        ooxmlPackage.close();
      } catch (IOException e) { //NOPMD
        // Nothing to do here
      }
    }
  }

}
