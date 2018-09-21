package com.profitera.dc.msxls.impl;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.profitera.dcpoi3_9.ss.formula.FormulaParseException;
import com.profitera.dcpoi3_9.ss.usermodel.Cell;
import com.profitera.dcpoi3_9.ss.usermodel.Row;

final class ExcelSheetRowIterator implements Iterator<Map<String, String>> {
  private final Iterator<Row> rows;
  private final static char[] AZ = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
    'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
    'W', 'X', 'Y', 'Z'};

  public ExcelSheetRowIterator(Iterator<Row> rows) {
    this.rows = rows;
  }

  @Override
  public boolean hasNext() {
    return rows.hasNext();
  }

  @Override
  public Map<String, String> next() {
    Row row = rows.next();
    Iterator<Cell> cells = row.cellIterator();
    Map<String, String> rowHashMap = new HashMap<String, String>();
    DecimalFormat numericFormat = new DecimalFormat("0.00######");
    while (cells.hasNext()) {
      Cell cell = cells.next();
      int columnIndex = cell.getColumnIndex();
      String columnKey = String.valueOf(ExcelSheetRowIterator.AZ[columnIndex % ExcelSheetRowIterator.AZ.length]);
      // We are just turning the index into a base-26 number with digits from A-Z
      while (columnIndex >= ExcelSheetRowIterator.AZ.length) {
        columnIndex = columnIndex / ExcelSheetRowIterator.AZ.length - 1;
        columnKey = ExcelSheetRowIterator.AZ[columnIndex % ExcelSheetRowIterator.AZ.length] + columnKey;
      }
      if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
        rowHashMap.put(columnKey, cell.getStringCellValue());
      } else if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
        double numericCellValue = cell.getNumericCellValue();
        String formatted = numericFormat.format(numericCellValue);
        if (formatted.endsWith(".00")) {
          formatted = formatted.replace(".00", "");
        }
        rowHashMap.put(columnKey, formatted);
      } else if (cell.getCellType() == Cell.CELL_TYPE_FORMULA) {
        try {
          String formulaStr = String.valueOf(cell.getCellFormula());
          rowHashMap.put(columnKey, formulaStr);
        } catch (FormulaParseException e) { //NOPMD
          // Ignore formula errors
        }
      } else if (cell.getCellType() == Cell.CELL_TYPE_BLANK) {
        rowHashMap.put(columnKey, "");
      } else if (cell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
        String booleanStr = String.valueOf(cell.getBooleanCellValue());
        rowHashMap.put(columnKey, booleanStr);
      } else {
        String otherStr = cell.toString();
        rowHashMap.put(columnKey, otherStr);
      }
    }
    return rowHashMap;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove() not supported for data loading");
  }
}