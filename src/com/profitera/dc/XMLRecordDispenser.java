package com.profitera.dc;

import java.io.BufferedInputStream;
import java.io.IOException;

import com.profitera.dc.filehandler.XMLRecordIterator;
import com.profitera.util.RecordDispenser;

public class XMLRecordDispenser extends RecordDispenser {
	public XMLRecordDispenser (BufferedInputStream inStr, int recordsPerRequest, String fileName, String recordTag) throws IOException {
    perRequest = recordsPerRequest;
    if (fileName != null && fileName.trim().length() != 0)
      this.sourceName = fileName;
			try {
        records = new XMLRecordIterator(inStr, recordTag);
      } catch (Exception e) {
        throw new IOException(e);
      }
	}

	public XMLRecordDispenser (BufferedInputStream inStr, int recordsPerRequest, String fileName, String recordTag, String xquery) throws IOException {
		perRequest = recordsPerRequest;
    if (fileName != null && fileName.trim().length() != 0) {
      this.sourceName = fileName;
    }
    try {
      records = new XMLRecordIterator(inStr, recordTag, xquery);
    } catch (Exception e) {
      throw new IOException(e);
    }
	}

	public String getType(){
		return RecordDispenserFactory.XML;
	}

  @Override
  public void terminate() {
    // Do nothing
  }
}