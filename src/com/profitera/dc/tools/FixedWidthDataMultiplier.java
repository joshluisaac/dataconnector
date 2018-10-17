package com.profitera.dc.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import com.profitera.dc.parser.V2LoadDefinitionParser;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.dc.parser.impl.Location;
import com.profitera.io.Base64;
import com.profitera.util.Strings;
import com.profitera.util.Utilities;
import com.profitera.util.io.FileUtil;
import com.profitera.util.io.ForEachLine;
import com.profitera.util.xml.DocumentLoader;

public class FixedWidthDataMultiplier {
  private final String multiplyFieldName;
  private final File sourceData;
  private final File destData;
  private final LoadDefinition definition;
  private final String keyFieldName;

  public FixedWidthDataMultiplier(Properties p) {
    String loadConf = p.getProperty("load");
    this.multiplyFieldName = p.getProperty("multiply");
    this.keyFieldName = p.getProperty("key");
    this.sourceData = new File(p.getProperty("source"));
    this.destData = new File(p.getProperty("destination"));
    File file = new File(loadConf);
    definition = V2LoadDefinitionParser.parse(DocumentLoader.loadDocument(file), FileUtil.tryCanonical(file));
  }

  public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
    int count = Integer.parseInt(args[0]);
    for (int j = 1; j < args.length; j++) {
      final FixedWidthDataMultiplier m = new FixedWidthDataMultiplier(Utilities.loadFromPath(args[j]));
      m.multiply(count);
    }
  }
  private void multiply(final int multiplyBy) throws IOException, NoSuchAlgorithmException {
    final FieldDefinition field = definition.getField(multiplyFieldName);
    final FieldDefinition keyField = keyFieldName != null && !keyFieldName.equals(multiplyFieldName) ? definition.getField(keyFieldName) : null;
    Location location = field.getLocation();
    final int start = Integer.parseInt(location.getStart());
    final int end = Integer.parseInt(location.getEnd());
    final MessageDigest digest = MessageDigest.getInstance("SHA");
    BufferedReader r = new BufferedReader(new FileReader(sourceData));
    final FileWriter destination = new FileWriter(destData);
    ForEachLine l = new ForEachLine(r) {
      @Override
      protected void process(String line) {
        try {
          if (definition.isHeader(line) || definition.isTrailer(line)) {
            destination.write(line + "\n");
          } else {
            for (int i = 0; i < multiplyBy; i++) {
              int recordId = i+1;
              String changedLine = adjustFieldValue(recordId, start, end, line, digest);
              if (keyField!=null) {
                Location keyLoc = keyField.getLocation();
                changedLine = adjustFieldValue(recordId, Integer.parseInt(keyLoc.getStart()), 
                    Integer.parseInt(keyLoc.getEnd()), changedLine, digest);
              }
              destination.write(changedLine + "\n");
            }
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
   l.process();
   r.close();
   destination.close();
  }

  private String adjustFieldValue(int recordId, final int start,
      final int end, String line, final MessageDigest digest)
      throws UnsupportedEncodingException {
    try {
      if (line.length() < end) {
        line = Strings.pad(line, end);
      }
    String before = line.substring(0, start - 1);
    String multiplierFieldValue = line.substring(start - 1, end);
    String after = line.substring(end);
    byte[] d = digest.digest((multiplierFieldValue + "_" + recordId).getBytes("UTF8"));
    digest.reset();
    String recordDigest = Base64.encodeBytes(d);
    String changedLine = before + Strings.pad(recordDigest, end-start + 1, '0') + after;
    return changedLine;
    } catch (StringIndexOutOfBoundsException e) {
      throw new IllegalArgumentException(recordId + " failed from " + start + " to " + end + " for '" + line + "' from " + FileUtil.tryCanonical(sourceData));
    }
  }
}