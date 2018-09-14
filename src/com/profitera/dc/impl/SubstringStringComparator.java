package com.profitera.dc.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.util.Strings;

public final class SubstringStringComparator implements Comparator<String> {
  private final int[] positions;
  private final LoadDefinition loadDefinition;
  public SubstringStringComparator(LoadDefinition def) {
    this.loadDefinition = def;
    FieldDefinition[] allKeys = def.getAllKeys();
    List<Integer> keyPositions = new ArrayList<>();
    for (int i = 0; i < allKeys.length; i++) {
      int start = Integer.valueOf(allKeys[i].getLocation().getStart());
      int end = Integer.valueOf(allKeys[i].getLocation().getEnd());
      if (start == 0) {
        continue;
      }
      keyPositions.add(start);
      keyPositions.add(end);
    }
    int[] positionArray = new int[keyPositions.size()];
    for (int i = 0; i < positionArray.length; i++) {
      positionArray[i] = keyPositions.get(i);
    }
    this.positions = positionArray;
  }
  @Override
  public int compare(String o1, String o2) {
    // Ordering specifically matters here, results need to be absolutely repeatable
    // even if o1 and o2 are swapped.
    if (loadDefinition.isHeader(o1)) {
      if (loadDefinition.isHeader(o2)) {
        return 0;
      } else {
        return -1;
      }
    }
    if (loadDefinition.isTrailer(o1)) {
      if (loadDefinition.isTrailer(o2)) {
        return 0;
      } else {
        return 1;
      }
    }
    if (loadDefinition.isHeader(o2)) {
      return -1;
    }
    if (loadDefinition.isTrailer(o2)) {
      return 1;
    }
    for (int i = 0; i < positions.length; i = i + 2) {
      int beginIndex = positions[i]-1;
      int endIndex = positions[i+1];
      if (o1.length() < endIndex) {
        o1 = Strings.pad(o1, endIndex);
      }
      if (o2.length() < endIndex) {
        o2 = Strings.pad(o2, endIndex);
      }
      String s1 = o1.substring(beginIndex, endIndex);
      String s2 = o2.substring(beginIndex, endIndex);
      int result = s1.compareTo(s2);
      if (result != 0) {
        return result;
      }
    }
    if (positions.length == 0) {
      return o1.compareTo(o2);
    }
    return 0;
  }
}