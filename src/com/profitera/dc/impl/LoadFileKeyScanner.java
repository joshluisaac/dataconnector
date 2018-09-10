package com.profitera.dc.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.profitera.dc.DataLoader;
import com.profitera.dc.InvalidLookupQueryException;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.util.IRecordDispenser;
import com.profitera.util.Strings;

public class LoadFileKeyScanner {
  private final LoadDefinition def;
  private Log log;

  public LoadFileKeyScanner(LoadDefinition def) {
    this.def = def;
  }

  public BigInteger scan(IRecordDispenser d) throws IOException, InvalidLookupQueryException {
    String threadName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName("filekeyscanner");
      List<FieldDefinition> keyFields = getKeys(def);
      Set<BigInteger> hashes = new HashSet<BigInteger>(10000);
      List<Object> l = new ArrayList<Object>();
      int lineNo = d.dispenseRecords(l);
      MessageDigest digester = getDigester();
      while (l.size() > 0) {
        for (int i = 0; i < l.size(); i++) {
          Object line = l.get(i);
          BigInteger lineHash = getLineKeyHash(line, keyFields, lineNo,
              digester);
          if (!hashes.add(lineHash)) {
            String[] lineKeyValues = getLineKeyValues(line, keyFields, lineNo);
            getLog().warn(
                "Found key hash collision on line " + (lineNo + i)
                    + " with keys " + Strings.getListString(keyFields, ", ")
                    + " and values "
                    + Strings.getListString(lineKeyValues, ", "));
            d.terminate();
            return lineHash;
          }
        }
        lineNo = d.dispenseRecords(l);
      }
      return null;
    } finally {
      Thread.currentThread().setName(threadName);
    }
  }

  public int[] findCollisions(BigInteger collision, IRecordDispenser d)
      throws IOException, InvalidLookupQueryException {
    String threadName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName("filekeyscanner");
      List<FieldDefinition> keyFields = getKeys(def);
      List<Integer> lines = new ArrayList<Integer>();
      List<Object> l = new ArrayList<Object>();
      int lineNo = d.dispenseRecords(l);
      MessageDigest digester = getDigester();
      while (l.size() > 0) {
        for (int i = 0; i < l.size(); i++) {
          Object line = l.get(i);
          BigInteger lineHash = getLineKeyHash(line, keyFields, lineNo,
              digester);
          if (lineHash.compareTo(collision) == 0) {
            lines.add(lineNo + i);
            String[] lineKeyValues = getLineKeyValues(line, keyFields, lineNo);
            getLog().warn(
                "Found duplicate key on line " + (lineNo + i) + " with keys "
                    + Strings.getListString(keyFields, ", ") + " and values "
                    + Strings.getListString(lineKeyValues, ", "));
            if (lines.size() == 50) {
              d.terminate();
              break;
            }
          }
        }
        lineNo = d.dispenseRecords(l);
      }
      int[] lineArray = new int[lines.size()];
      for (int i = 0; i < lineArray.length; i++) {
        lineArray[i] = lines.get(i);
      }
      return lineArray;
    } finally {
      Thread.currentThread().setName(threadName);
    }
  }

  private List<FieldDefinition> getKeys(LoadDefinition d)
      throws InvalidLookupQueryException {
    List<FieldDefinition> fields = d.getAllFields();
    Set<String> keyText = new HashSet<String>();
    for (FieldDefinition f : fields) {
      if (f.isKey() && f.isLookupField()) {
        String[] lookupParams = f.getLookupDefinition().getLookupQueryParams();
        // We can't just add all the params because they might be #VALUE# or
        // #CODE#
        // in which case the name of the field needs to be added, not the param
        // name.
        for (int i = 0; i < lookupParams.length; i++) {
          if (DataLoader.isNotSelfLookup(lookupParams[i], f)) {
            keyText.add(lookupParams[i]);
          } else {
            keyText.add(f.getFieldName());
          }
        }
      } else if (f.isKey()) {
        keyText.add(f.getFieldName());
      }
    }
    List<FieldDefinition> keyFields = new ArrayList<FieldDefinition>();
    for (String name : keyText) {
      keyFields.add(def.getField(name));
    }
    return keyFields;
  }

  private BigInteger getLineKeyHash(Object line,
      List<FieldDefinition> keyFields, int lineNo, MessageDigest digester)
      throws UnsupportedEncodingException {
    String[] lineKeyValues = getLineKeyValues(line, keyFields, lineNo);
    String allKeyText = Strings.getListString(lineKeyValues, "/");
    digester.reset();
    digester.update(allKeyText.getBytes("UTF8"));
    BigInteger lineHash = new BigInteger(digester.digest());
    return lineHash;
  }

  private String[] getLineKeyValues(Object line,
      List<FieldDefinition> keyFields, int lineNo) {
    String[] delimitedFields = DataLoader.getDelimitedFields(line, def);
    String[] lineKeyValues = new String[keyFields.size()];
    for (int index = 0; index < keyFields.size(); index++) {
      FieldDefinition fd = keyFields.get(index);
      lineKeyValues[index] = DataLoader.getLineValueText(line, lineNo,
          delimitedFields, fd, def);
    }
    return lineKeyValues;
  }

  private MessageDigest getDigester() throws IOException {
    MessageDigest digest = null;
    try {
      digest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e.getMessage());
    }
    return digest;
  }

  private Log getLog() {
    if (log == null) {
      log = LogFactory.getLog(getClass());
    }
    return log;
  }
}
